package com.dynatrace.easytrade.creditcardorderservice;

import com.dynatrace.easytrade.creditcardorderservice.models.ManufactureRequest;
import com.dynatrace.easytrade.creditcardorderservice.models.StandardResponse;
import com.dynatrace.easytrade.creditcardorderservice.models.StatusType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Service
public class WorkScheduler extends BaseScheduler {
    private static final Logger logger = LoggerFactory.getLogger(WorkScheduler.class);
    private final DatabaseHelper dbHelper;
    private final String creditCardOrderService = System.getenv("THIRD_PARTY_SERVICE_HOSTANDPORT");
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(envInt("MANUFACTURER_CONNECT_TIMEOUT_S", 10)))
            .build();
    // Previously the scheduler called the manufacturer one order at a time, blocking on each
    // synchronous HTTP call before starting the next; a backlog of N orders took N sequential
    // round-trips. Orders are now dispatched concurrently on a bounded pool so throughput no
    // longer depends on a single thread, while the concurrency cap protects the downstream
    // third-party service (and our own resources) from being overwhelmed.
    private final int maxConcurrency = envInt("MANUFACTURER_MAX_CONCURRENCY", 8);
    private final int requestTimeoutSeconds = envInt("MANUFACTURER_REQUEST_TIMEOUT_S", 30);

    public WorkScheduler(DatabaseHelper dbHelper) {
        super("work", Integer.parseInt(System.getenv("WORK_DELAY")), Integer.parseInt(System.getenv("WORK_RATE")));
        this.dbHelper = dbHelper;
    }

    @Override
    protected void run() {
        logger.info("Running WorkScheduler task!");

        try (Connection conn = dbHelper.getConnection()) {
            List<String> orders = dbHelper.getOrderIdsWithStatusOrderCreated(conn);

            if (!orders.isEmpty()) {
                dispatchManufactureRequests(conn, orders);
            }
        } catch (Exception e) {
            logger.error("Caught exception while running WorkScheduler's task: " + e.getMessage(), e);
        }

        logger.info("Finished WorkScheduler task!");

        randomFixedRatePlusSleep();
    }

    private void dispatchManufactureRequests(Connection conn, List<String> orders) throws Exception {
        // Read the per-order manufacture data up front on the scheduler's connection (JDBC
        // connections are not thread-safe), then fan out the independent HTTP calls.
        Semaphore concurrencyLimit = new Semaphore(maxConcurrency);
        ObjectMapper mapper = new ObjectMapper();
        int poolSize = Math.min(maxConcurrency, orders.size());
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        try {
            for (String orderId : orders) {
                ManufactureRequest body = dbHelper.getCreditCardManufactureData(conn, orderId);
                String payload = mapper.writeValueAsString(body);
                concurrencyLimit.acquire();
                executor.submit(() -> {
                    try {
                        callManufacturer(orderId, payload);
                    } finally {
                        concurrencyLimit.release();
                    }
                });
            }
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                logger.warn("WorkScheduler manufacture dispatch did not finish within timeout; forcing shutdown.");
                executor.shutdownNow();
            }
        }
    }

    private void callManufacturer(String orderId, String payload) {
        logger.info("Calling manufacturer for order: " + orderId);
        try {
            ObjectMapper mapper = new ObjectMapper();
            // deepcode ignore Ssrf: trusted environment variable
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(String.format("http://%s/v1/manufacturer", creditCardOrderService)))
                    .timeout(Duration.ofSeconds(requestTimeoutSeconds))
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .header("Content-Type", "application/json")
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            StandardResponse standardResponse = mapper.readValue(response.body(), StandardResponse.class);

            if (standardResponse.statusCode() == HttpStatus.OK.value()) {
                logger.info("Ordered the manufacture of card with orderId: {}", orderId);
                dbHelper.insertNewStatus(orderId, StatusType.CARD_ORDERED);
            } else {
                logger.info("Failed to order card manufacture. Status code received is {}, and the message is: {}",
                        standardResponse.statusCode(), standardResponse.message());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while calling manufacturer for order {}: {}", orderId, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Failed to call manufacturer for order {}: {}", orderId, e.getMessage(), e);
        }
    }

    private static int envInt(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid integer for env var {}='{}', using default {}", name, value, defaultValue);
            return defaultValue;
        }
    }
}

package com.dynatrace.easytrade.creditcardorderservice;

import com.dynatrace.easytrade.creditcardorderservice.models.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class DatabaseHelper {
    private final String INSERT_ORDER_QUERY = "INSERT INTO [dbo].[CreditCardOrders] ([Id], [AccountId], [Email], [Name], [ShippingAddress], [CardLevel]) VALUES (?, ?, ?, ?, ?, ?)";
    private final String INSERT_STATUS_QUERY = "INSERT INTO [dbo].[CreditCardOrderStatus] ([CreditCardOrderId], [Timestamp], [Status], [Details]) VALUES (?, ?, ?, ?)";
    private final String INSERT_CREDIT_CARD_QUERY = "INSERT INTO [dbo].[CreditCards] ([CreditCardOrderId], [Level], [Number], [Cvs], [ValidDate]) VALUES (?, ?, ?, ?, ?)";
    private final String UPDATE_ORDER_QUERY = "UPDATE CreditCardOrders SET ShippingId = ? WHERE Id = ?";
    private final String COUNT_ORDER_BY_ACCOUNT_ID_QUERY = "SELECT COUNT(*) FROM CreditCardOrders WHERE AccountId = ?";
    private final String SHIPPING_ADDRESS_DATA_QUERY = "SELECT Name, Email, ShippingAddress FROM CreditCardOrders WHERE Id = ?";
    private final String CC_MANUFACTURE_DETAILS_QUERY = "SELECT Id, Name, CardLevel FROM CreditCardOrders WHERE Id = ?";
    private final String GET_ORDER_BY_ACCOUNT_ID = "SELECT Id FROM CreditCardOrders WHERE AccountId = ?";
    private final String GET_LAST_STATUS_QUERY = "SELECT TOP 1 * FROM CreditCardOrderStatus WHERE CreditCardOrderId = ? ORDER BY Timestamp DESC";
    private final String GET_STATUS_LIST_QUERY = "SELECT * FROM CreditCardOrderStatus WHERE CreditCardOrderId = ? ORDER BY Timestamp DESC";
    private final String GET_LAST_STATUS_BY_ACCOUNT_ID_QUERY = "SELECT TOP 1 * FROM CreditCardOrderStatus ccos WHERE ccos.CreditCardOrderId = (SELECT cco.Id FROM CreditCardOrders cco WHERE cco.AccountId = ?) ORDER BY Timestamp DESC";
    private final String DELETE_ORDER_STATUS_BY_ACCOUNT_ID_QUERY = "DELETE FROM CreditCardOrderStatus WHERE CreditCardOrderId = (SELECT y.Id FROM CreditCardOrders y WHERE y.AccountId = ?)";
    private final String DELETE_CREDIT_CARD_BY_ACCOUNT_ID_QUERY = "DELETE FROM CreditCards WHERE CreditCardOrderId = (SELECT y.Id FROM CreditCardOrders y WHERE y.AccountId = ?)";
    private final String DELETE_ORDER_BY_ACCOUNT_ID_QUERY = "DELETE FROM CreditCardOrders WHERE AccountId = ?";
    private final String GET_ORDER_ID_AND_CURRENT_STATUS = "select x.CreditCardOrderId, x.Status from CreditCardOrderStatus x inner join (select max(Id) Id, CreditCardOrderId from CreditCardOrderStatus group by CreditCardOrderId) y on x.CreditCardOrderId = y.CreditCardOrderId and x.Id = y.Id";
    // Same "latest status per order" logic as above, but the ORDER_CREATED filter is applied in
    // SQL so only matching order ids leave the database. The previous approach loaded every
    // order's current status into memory and filtered in Java, which scans linearly with the
    // full order history and does not scale.
    private final String GET_ORDER_IDS_WITH_CURRENT_STATUS = "select x.CreditCardOrderId from CreditCardOrderStatus x inner join (select max(Id) Id, CreditCardOrderId from CreditCardOrderStatus group by CreditCardOrderId) y on x.CreditCardOrderId = y.CreditCardOrderId and x.Id = y.Id where x.Status = ?";
    private static final Logger logger = LoggerFactory.getLogger(DatabaseHelper.class);

    public String insertNewOrder(Connection conn, CreditCardOrderRequest request) throws SQLException {
        PreparedStatement query = conn.prepareStatement(INSERT_ORDER_QUERY);
        String guid = UUID.randomUUID().toString();
        query.setString(1, guid);
        query.setInt(2, request.accountId());
        query.setString(3, request.email());
        query.setString(4, request.name());
        query.setString(5, request.shippingAddress());
        query.setString(6, request.cardLevel());
        query.executeUpdate();
        query.close();
        return guid;
    }

    public Optional<ShippingAddressResponse> getShippingAddress(Connection conn, String creditCardOrderId)
            throws SQLException {
        PreparedStatement query = conn.prepareStatement(SHIPPING_ADDRESS_DATA_QUERY);
        query.setString(1, creditCardOrderId);
        ResultSet rs = query.executeQuery();
        if (!rs.next()) {
            query.close();
            return Optional.empty();
        }
        var result = Optional.of(new ShippingAddressResponse(
                rs.getString("ShippingAddress"),
                rs.getString("Name"),
                rs.getString("Email")));
        query.close();
        return result;
    }

    public ManufactureRequest getCreditCardManufactureData(Connection conn, String creditCardOrderId)
            throws SQLException {
        PreparedStatement query = conn.prepareStatement(CC_MANUFACTURE_DETAILS_QUERY);
        query.setString(1, creditCardOrderId);
        ResultSet rs = query.executeQuery();
        if (!rs.next()) {
            query.close();
            return null;
        }
        var result = new ManufactureRequest(
                rs.getString("Id"),
                rs.getString("Name"),
                rs.getString("CardLevel"));
        query.close();
        return result;
    }

    public Integer getOrderCountForAccountId(Connection conn, Integer accountId) throws SQLException {
        PreparedStatement query = conn.prepareStatement(COUNT_ORDER_BY_ACCOUNT_ID_QUERY);
        query.setInt(1, accountId);
        ResultSet rs = query.executeQuery();
        rs.next();
        var result = rs.getInt(1);
        query.close();
        return result;
    }

    public Optional<String> getOrderIdForAccount(Connection conn, Integer accountId) throws SQLException {
        PreparedStatement query = conn.prepareStatement(GET_ORDER_BY_ACCOUNT_ID);
        query.setInt(1, accountId);
        ResultSet rs = query.executeQuery();

        Optional<String> result;
        if (rs.next()) {
            result = Optional.of(rs.getString("Id"));
        } else {
            result = Optional.empty();
        }
        query.close();
        return result;
    }

    public List<CreditCardOrderStatus> getOrderStatusList(Connection conn, String creditCardOrderId)
            throws SQLException {
        PreparedStatement query = conn.prepareStatement(GET_STATUS_LIST_QUERY);
        query.setString(1, creditCardOrderId);
        ResultSet rs = query.executeQuery();

        List<CreditCardOrderStatus> result = new ArrayList<>();
        while (rs.next()) {
            result.add(CreditCardOrderStatus.fromResultSet(rs));
        }
        query.close();
        return result;
    }

    public CreditCardOrderStatus getLastOrderStatus(Connection conn, String creditCardOrderId) throws SQLException {
        PreparedStatement query = conn.prepareStatement(GET_LAST_STATUS_QUERY);
        query.setString(1, creditCardOrderId);
        ResultSet rs = query.executeQuery();

        CreditCardOrderStatus status = null;
        if (rs.next()) {
            status = CreditCardOrderStatus.fromResultSet(rs);
        }
        query.close();
        return status;
    }

    public List<String> getOrderIdsWithStatusOrderCreated(Connection conn) throws SQLException {
        try (PreparedStatement query = conn.prepareStatement(GET_ORDER_IDS_WITH_CURRENT_STATUS)) {
            query.setString(1, StatusType.ORDER_CREATED.getType());
            try (ResultSet rs = query.executeQuery()) {
                ArrayList<String> orders = new ArrayList<>();
                while (rs.next()) {
                    orders.add(rs.getString(1));
                }
                return orders;
            }
        }
    }

    public void insertNewStatus(String orderId, StatusType statusType) throws SQLException {
        try (Connection conn = getConnection()) {
            insertNewStatus(conn, orderId, statusType, statusType.getDescription());
        }
    }

    public void insertNewStatus(String orderId, StatusType statusType, String details) throws SQLException {
        try (Connection conn = getConnection()) {
            insertNewStatus(conn, orderId, statusType, details);
        }
    }

    public void insertNewStatus(Connection conn, String orderId, StatusType statusType) throws SQLException {
        insertNewStatus(conn, orderId, statusType, statusType.getDescription());
    }

    public void insertNewStatus(Connection conn, String orderId, StatusType statusType, String details)
            throws SQLException {
        Timestamp timestamp = Timestamp.valueOf(OffsetDateTime.now().toLocalDateTime());
        logger.debug("Inserting new status with [orderID::" + orderId + "] [date::" + timestamp + "] [status::"
                + statusType.getType() + "] [details::" + details + "]");
        PreparedStatement query = conn.prepareStatement(INSERT_STATUS_QUERY);
        query.setString(1, orderId);
        query.setTimestamp(2, timestamp);
        query.setString(3, statusType.getType());
        query.setString(4, details);
        query.executeUpdate();
        query.close();
    }

    public void insertNewCreditCard(String orderId, CreditCardRequest request) throws SQLException {
        try (Connection conn = getConnection()) {
            PreparedStatement query = conn.prepareStatement(INSERT_CREDIT_CARD_QUERY);
            query.setString(1, orderId);
            query.setString(2, request.cardLevel());
            query.setString(3, request.cardNumber());
            query.setString(4, request.cardCVS());
            query.setTimestamp(5, Timestamp.valueOf(request.cardValidDate().toLocalDateTime()));
            query.executeUpdate();
            query.close();
        }
    }

    public void updateOrder(String orderId, ShippingIdRequest shippingIdRequest) throws SQLException {
        try (Connection conn = getConnection()) {
            PreparedStatement query = conn.prepareStatement(UPDATE_ORDER_QUERY);
            query.setString(1, shippingIdRequest.shippingId());
            query.setString(2, orderId);
            query.executeUpdate();
            query.close();
        }
    }

    /**
     * Pooled data source. Previously every call opened a brand-new physical connection via
     * {@link DriverManager#getConnection(String)}, which does not survive high concurrency:
     * connection setup/teardown per operation exhausts SQL Server's connection budget and adds
     * per-request latency. HikariCP keeps a bounded pool of reusable connections so throughput
     * scales without unbounded connection churn.
     *
     * <p>Lazily initialized so the service still starts when the DB is briefly unavailable, and
     * so behaviour is unchanged for callers other than the connection source. Guarded by a lock
     * because both the web request threads and the {@code WorkScheduler} obtain connections.
     */
    private volatile HikariDataSource dataSource;

    private HikariDataSource getDataSource() {
        HikariDataSource local = dataSource;
        if (local == null) {
            synchronized (this) {
                local = dataSource;
                if (local == null) {
                    local = buildDataSource();
                    dataSource = local;
                }
            }
        }
        return local;
    }

    private HikariDataSource buildDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(System.getenv("MSSQL_CONNECTIONSTRING"));
        config.setPoolName("credit-card-order-service-pool");
        // Bounded pool: sized from env with safe defaults so it can be tuned per environment
        // without a code change. Keep max pool size aligned with the DB's connection budget.
        config.setMaximumPoolSize(envInt("DB_POOL_MAX_SIZE", 20));
        config.setMinimumIdle(envInt("DB_POOL_MIN_IDLE", 2));
        config.setConnectionTimeout(envLong("DB_POOL_CONNECTION_TIMEOUT_MS", 30_000L));
        config.setIdleTimeout(envLong("DB_POOL_IDLE_TIMEOUT_MS", 600_000L));
        config.setMaxLifetime(envLong("DB_POOL_MAX_LIFETIME_MS", 1_800_000L));
        logger.info("Initializing HikariCP connection pool [{}] with maxPoolSize={}, minIdle={}",
                config.getPoolName(), config.getMaximumPoolSize(), config.getMinimumIdle());
        return new HikariDataSource(config);
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

    private static long envLong(String name, long defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid long for env var {}='{}', using default {}", name, value, defaultValue);
            return defaultValue;
        }
    }

    public Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    @PreDestroy
    public void close() {
        HikariDataSource local = dataSource;
        if (local != null && !local.isClosed()) {
            logger.info("Closing HikariCP connection pool [{}]", local.getPoolName());
            local.close();
        }
    }

    public Optional<CreditCardOrderStatus> getLastOrderStatusForAccountId(Connection conn, Integer accountId)
            throws SQLException {
        PreparedStatement query = conn.prepareStatement(GET_LAST_STATUS_BY_ACCOUNT_ID_QUERY);
        query.setInt(1, accountId);
        ResultSet rs = query.executeQuery();

        Optional<CreditCardOrderStatus> result;
        if (rs.next()) {
            result = Optional.of(CreditCardOrderStatus.fromResultSet(rs));
        } else {
            result = Optional.empty();
        }
        query.close();
        return result;
    }

    public void deleteOrderForAccountId(Connection conn, Integer accountId) throws SQLException {
        deleteByAccountId(conn, accountId, DELETE_ORDER_STATUS_BY_ACCOUNT_ID_QUERY);
        deleteByAccountId(conn, accountId, DELETE_CREDIT_CARD_BY_ACCOUNT_ID_QUERY);
        deleteByAccountId(conn, accountId, DELETE_ORDER_BY_ACCOUNT_ID_QUERY);
    }

    private void deleteByAccountId(Connection conn, Integer accountId, String deleteQuery) throws SQLException {
        PreparedStatement query = conn.prepareStatement(deleteQuery);
        query.setInt(1, accountId);
        query.executeUpdate();
        query.close();
    }
}

package ravenworks.magpie.engine.source.mysql;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.json.JsonUtils;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.engine.source.SourceConnector;
import ravenworks.magpie.engine.stream.MessageRecord;
import ravenworks.magpie.engine.stream.SendResult;
import ravenworks.magpie.engine.stream.StreamProducer;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * @author Raven
 */
@Slf4j
public class MySqlPollSourceConnector implements SourceConnector {

    private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {

    };

    private static final int DEFAULT_POLL_INTERVAL_MS = 5_000;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_RETRY_DELAY_MS = 300_000;
    private static final Object POLL_SIGNAL = new Object();

    private final String name;
    private final StreamProducer producer;
    private final String url;
    private final String username;
    private final String password;
    private final int batchSize;
    private final int retryDelay;
    private final EventLoop eventLoop;

    private Connection connection;
    private long availableAt;

    public MySqlPollSourceConnector(@NonNull StreamProducer producer,
                                    @NonNull String name,
                                    @NonNull Map<String, Object> properties) {
        this.name = name;
        this.producer = producer;
        this.url = getStringProperty(properties, "url", null);
        if (this.url == null || this.url.isEmpty()) {
            throw new IllegalArgumentException("Property 'url' is required for MySQL poll source");
        }
        this.username = getStringProperty(properties, "username", "");
        this.password = getStringProperty(properties, "password", "");
        this.batchSize = getIntProperty(properties, "batchSize", DEFAULT_BATCH_SIZE);
        this.retryDelay = getIntProperty(properties, "retryDelay", DEFAULT_RETRY_DELAY_MS);
        int pollInterval = getIntProperty(properties, "pollInterval", DEFAULT_POLL_INTERVAL_MS);
        this.eventLoop = new EventLoop("src-" + name, pollInterval, this::dispatch);
    }

    @Override
    public String type() {
        return "mysql-poll";
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void start() {
        this.eventLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return this.eventLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event instanceof EventLoop.Idle) {
            this.eventLoop.enqueue(POLL_SIGNAL);
        } else if (event == POLL_SIGNAL) {
            try {
                this.doPoll();
            } catch (Exception e) {
                log.error("Poll failed for source '{}'", this.name, e);
                this.availableAt = System.currentTimeMillis() + this.retryDelay;
            }
        } else if (event instanceof EventLoop.PreShutdown) {
            this.closeConnection();
        }
    }

    private void doPoll() {
        if (System.currentTimeMillis() < this.availableAt) {
            return;
        }
        if (!ensureConnection()) {
            return;
        }
        var records = queryBatch();
        if (records.isEmpty()) {
            return;
        }
        var futures = sendBatch(records);
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

        boolean allSucceeded = true;
        for (var future : futures) {
            SendResult result = future.getNow(null);
            if (result == null || !result.isSucceeded()) {
                allSucceeded = false;
                break;
            }
        }
        if (allSucceeded) {
            deleteBatch(records);
            if (records.size() == this.batchSize) {
                this.eventLoop.enqueue(POLL_SIGNAL);
            }
        } else {
            this.availableAt = System.currentTimeMillis() + this.retryDelay;
            log.error("Send batch failed for source '{}', retry after {}", this.name, this.availableAt);
        }
    }

    private List<OutboxRecord> queryBatch() {
        List<OutboxRecord> records = new ArrayList<>();
        String sql = "SELECT id, type, time, tenant_id, topic, partition_key, headers, payload FROM magpie_outbox_message ORDER BY id ASC, time ASC LIMIT ?";
        try (PreparedStatement ps = this.connection.prepareStatement(sql)) {
            ps.setInt(1, this.batchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    var record = new OutboxRecord();
                    record.id = rs.getString("id");
                    record.type = rs.getString("type");
                    Timestamp ts = rs.getTimestamp("time");
                    record.time = ts != null ? ts.toLocalDateTime() : null;
                    record.tenantId = rs.getString("tenant_id");
                    record.topic = rs.getString("topic");
                    record.partitionKey = rs.getString("partition_key");
                    String headersJson = rs.getString("headers");
                    record.headers = JsonUtils.decode(headersJson, HEADERS_TYPE);
                    record.payload = rs.getString("payload");
                    records.add(record);
                }
            }
        } catch (SQLException e) {
            log.error("Query batch failed for source '{}'", this.name, e);
            this.connection = null;
        }
        return records;
    }

    private List<CompletableFuture<SendResult>> sendBatch(List<OutboxRecord> records) {
        List<CompletableFuture<SendResult>> futures = new ArrayList<>();
        for (var r : records) {
            var msg = new MessageRecord()
                    .setId(r.id)
                    .setType(r.type)
                    .setTime(r.time)
                    .setTenantId(r.tenantId)
                    .setTopic(r.topic)
                    .setPartitionKey(r.partitionKey)
                    .setHeaders(r.headers)
                    .setPayload(r.payload != null ? r.payload.getBytes(StandardCharsets.UTF_8) : new byte[0]);
            futures.add(this.producer.send(msg));
        }
        return futures;
    }

    private void deleteBatch(List<OutboxRecord> records) {
        List<String> ids = records.stream()
                .map(r -> r.id)
                .toList();
        String placeholders = records.stream()
                .map(r -> "?")
                .collect(Collectors.joining(","));
        String sql = "DELETE FROM magpie_outbox_message WHERE id IN (" + placeholders + ")";
        try (PreparedStatement ps = this.connection.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setString(i + 1, ids.get(i));
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Delete batch failed for source '{}'", this.name, e);
            this.connection = null;
        }
    }

    private boolean ensureConnection() {
        if (this.connection != null) {
            try {
                if (!this.connection.isClosed() && this.connection.isValid(2)) {
                    return true;
                }
            } catch (SQLException ignored) {
            }
            closeConnection();
        }
        try {
            this.connection = DriverManager.getConnection(this.url, this.username, this.password);
            log.info("Connected to MySQL for source '{}'", this.name);
            return true;
        } catch (SQLException e) {
            log.error("Failed to connect to MySQL for source '{}': {}", this.name, e.getMessage());
            return false;
        }
    }

    private void closeConnection() {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException ignored) {
            }
            this.connection = null;
        }
    }

    private static int getIntProperty(Map<String, Object> properties, String key, int defaultValue) {
        Object value = properties.get(key);
        if (value instanceof Number n) {
            return n.intValue();
        }
        return defaultValue;
    }

    private static String getStringProperty(Map<String, Object> properties, String key, String defaultValue) {
        Object value = properties.get(key);
        if (value instanceof String s) {
            return s;
        }
        return defaultValue;
    }


    private static class OutboxRecord {

        String id;
        String type;
        LocalDateTime time;
        String tenantId;
        String topic;
        String partitionKey;
        Map<String, String> headers;
        String payload;

    }

}

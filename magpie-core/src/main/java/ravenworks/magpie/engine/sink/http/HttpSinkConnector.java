package ravenworks.magpie.engine.sink.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.engine.sink.SinkConnector;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.StreamConsumer;
import ravenworks.magpie.engine.stream.StreamDefinition;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Raven
 */
@Slf4j
public class HttpSinkConnector implements SinkConnector {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private static final String PROP_URL = "url";
    private static final String PROP_TIMEOUT = "timeout";
    private static final String PROP_RETRY_MAX_ATTEMPTS = "retry.maxAttempts";
    private static final String PROP_RETRY_BACKOFF = "retry.backoff";
    private static final String PROP_RETRY_DELAY = "retry.delay";
    private static final String PROP_RETRY_MAX_DELAY = "retry.maxDelay";
    private static final String PROP_RETRY_STATUS_CODES = "retry.statusCodes";

    private static final String DEFAULT_TIMEOUT = "10000";
    private static final String DEFAULT_MAX_ATTEMPTS = "3";
    private static final String DEFAULT_BACKOFF = "fixed";
    private static final String DEFAULT_DELAY = "1000";
    private static final String DEFAULT_MAX_DELAY = "30000";
    private static final String DEFAULT_STATUS_CODES = "500-599,408,429";

    private static final Set<String> RESERVED_CE_KEYS = Set.of(
            "specversion", "id", "source", "type", "time",
            "subject", "datacontenttype", "data", "headers"
    );

    private final StreamProvider provider;
    private final StreamRegistry streamRegistry;
    private final String name;
    private final String topic;
    private final HttpConfig config;
    private final List<HttpSinkWorker> workers = new ArrayList<>();

    public HttpSinkConnector(@NonNull StreamProvider provider,
                             @NonNull StreamRegistry streamRegistry,
                             @NonNull String name,
                             @NonNull String topic,
                             Map<String, Object> properties) {
        this.provider = provider;
        this.streamRegistry = streamRegistry;
        this.name = name;
        this.topic = topic;
        this.config = parseConfig(properties);
    }

    @Override
    public String type() {
        return "http";
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void start() {
        StreamDefinition definition = this.streamRegistry.getStream(this.topic);
        if (definition == null) {
            log.error("Stream not found: {}", this.topic);
            return;
        }
        var consumers = this.provider.consumer(definition, this.name);
        for (int i = 0; i < consumers.size(); i++) {
            var worker = new HttpSinkWorker(this.name, i, consumers.get(i), this.config);
            this.workers.add(worker);
            worker.start();
        }
        log.info("HTTP sink '{}' started, {} worker(s), url={}", this.name, this.workers.size(), this.config.url);
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        var futures = this.workers.stream()
                .map(HttpSinkWorker::shutdown)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenRun(() -> log.info("HTTP sink '{}' shutdown", this.name));
    }

    private static HttpConfig parseConfig(Map<String, Object> props) {
        if (props == null) {
            throw new IllegalArgumentException("HttpSinkConnector requires 'url' property");
        }
        String url = getString(props, PROP_URL, null);
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("HttpSinkConnector requires 'url' property");
        }
        int timeout = Integer.parseInt(getString(props, PROP_TIMEOUT, DEFAULT_TIMEOUT));
        int maxAttempts = Integer.parseInt(getString(props, PROP_RETRY_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS));
        String backoff = getString(props, PROP_RETRY_BACKOFF, DEFAULT_BACKOFF);
        long delay = Long.parseLong(getString(props, PROP_RETRY_DELAY, DEFAULT_DELAY));
        long maxDelay = Long.parseLong(getString(props, PROP_RETRY_MAX_DELAY, DEFAULT_MAX_DELAY));
        String statusCodesStr = getString(props, PROP_RETRY_STATUS_CODES, DEFAULT_STATUS_CODES);
        Set<Integer> retryStatusCodes = parseStatusCodes(statusCodesStr);
        return new HttpConfig(url, timeout, maxAttempts, backoff, delay, maxDelay, retryStatusCodes);
    }

    private static String getString(Map<String, Object> props, String key, String defaultValue) {
        Object val = props.get(key);
        if (val == null) {
            return defaultValue;
        }
        return val.toString();
    }

    private static Set<Integer> parseStatusCodes(String str) {
        Set<Integer> codes = new java.util.HashSet<>();
        for (String part : str.split(",")) {
            part = part.trim();
            if (part.contains("-")) {
                String[] range = part.split("-", 2);
                int start = Integer.parseInt(range[0].trim());
                int end = Integer.parseInt(range[1].trim());
                for (int c = start; c <= end; c++) {
                    codes.add(c);
                }
            } else if (!part.isEmpty()) {
                codes.add(Integer.parseInt(part));
            }
        }
        return codes;
    }

    private static String buildCloudEventJson(ConsumerRecord record) {
        Map<String, Object> ce = new LinkedHashMap<>();
        ce.put("specversion", "1.0");
        ce.put("id", record.getId());
        ce.put("source", "");
        ce.put("type", record.getType());
        if (record.getTime() != null) {
            ce.put("time", record.getTime().toString());
        }
        ce.put("subject", record.getTopic());
        ce.put("datacontenttype", "application/json");

        String payloadStr = record.getPayload() != null
                ? new String(record.getPayload(), StandardCharsets.UTF_8)
                : "";
        ce.put("data", payloadStr);

        Map<String, String> extHeaders = new LinkedHashMap<>();
        if (record.getHeaders() != null) {
            for (var entry : record.getHeaders().entrySet()) {
                if (!RESERVED_CE_KEYS.contains(entry.getKey().toLowerCase())) {
                    extHeaders.put(entry.getKey(), entry.getValue());
                }
            }
        }
        ce.put("headers", extHeaders);

        try {
            return OBJECT_MAPPER.writeValueAsString(ce);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize CloudEvent", e);
            return "{}";
        }
    }

    private static long computeBackoffDelay(String backoff, long delay, long maxDelay, int attempt) {
        if ("exponential".equalsIgnoreCase(backoff)) {
            long computed = delay * (1L << (attempt - 1));
            return Math.min(computed, maxDelay);
        }
        return delay;
    }

    private static boolean isRetryable(int statusCode, Set<Integer> retryStatusCodes) {
        return retryStatusCodes.contains(statusCode);
    }

    private static class HttpConfig {
        final String url;
        final int timeout;
        final int maxAttempts;
        final String backoff;
        final long delay;
        final long maxDelay;
        final Set<Integer> retryStatusCodes;

        HttpConfig(String url, int timeout, int maxAttempts, String backoff,
                   long delay, long maxDelay, Set<Integer> retryStatusCodes) {
            this.url = url;
            this.timeout = timeout;
            this.maxAttempts = maxAttempts;
            this.backoff = backoff;
            this.delay = delay;
            this.maxDelay = maxDelay;
            this.retryStatusCodes = retryStatusCodes;
        }
    }

    private static class HttpSinkWorker {

        private static final int BACKPRESSURE_LIMIT = 100;

        private final String name;
        private final int partition;
        private final StreamConsumer consumer;
        private final HttpConfig config;
        private final Semaphore semaphore = new Semaphore(BACKPRESSURE_LIMIT);
        private final AtomicLong received = new AtomicLong();
        private final EventLoop eventLoop;
        private final HttpClient httpClient;
        private volatile Thread eventLoopThread;
        private volatile boolean stopped;

        HttpSinkWorker(String name, int partition, StreamConsumer consumer, HttpConfig config) {
            this.name = name;
            this.partition = partition;
            this.consumer = consumer;
            this.config = config;
            this.eventLoop = new EventLoop("snk-" + name + "-" + partition, 5_000, this::dispatch);
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(config.timeout))
                    .build();
        }

        void start() {
            this.consumer.consume(0, record -> {
                this.eventLoop.enqueue(record);
                try {
                    this.semaphore.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            this.eventLoop.start();
        }

        CompletableFuture<Void> shutdown() {
            this.stopped = true;
            Thread t = this.eventLoopThread;
            if (t != null) {
                LockSupport.unpark(t);
            }
            try {
                this.consumer.close();
            } catch (Exception ignored) {
            }
            return this.eventLoop.shutdown();
        }

        private void dispatch(Object event) {
            if (event instanceof EventLoop.Started) {
                this.eventLoopThread = Thread.currentThread();
                return;
            }
            if (this.stopped) {
                return;
            }
            if (event instanceof ConsumerRecord record) {
                long count = this.received.incrementAndGet();
                String json = buildCloudEventJson(record);
                deliverWithRetry(json, count, record);
                this.semaphore.release();
            }
        }

        private void deliverWithRetry(String json, long count, ConsumerRecord record) {
            int attempt = 0;
            int maxAttempts = this.config.maxAttempts;
            while (!this.stopped) {
                attempt++;
                try {
                    var request = HttpRequest.newBuilder()
                            .uri(URI.create(this.config.url))
                            .timeout(Duration.ofMillis(this.config.timeout))
                            .header("Content-Type", "application/cloudevents+json")
                            .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                            .build();

                    var response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() >= 200 && response.statusCode() < 300) {
                        log.debug("[{}] partition={} offset={} count={} delivered, status={}",
                                this.name, this.partition, record.getOffset(), count, response.statusCode());
                        return;
                    }

                    if (attempt < maxAttempts && isRetryable(response.statusCode(), this.config.retryStatusCodes)) {
                        long delay = computeBackoffDelay(this.config.backoff, this.config.delay,
                                this.config.maxDelay, attempt);
                        log.warn("[{}] partition={} offset={} count={} HTTP {} (attempt {}/{}), retry in {}ms",
                                this.name, this.partition, record.getOffset(), count,
                                response.statusCode(), attempt, maxAttempts, delay);
                        LockSupport.parkNanos(delay * 1_000_000L);
                        continue;
                    }

                    log.error("[{}] partition={} offset={} count={} delivery failed, HTTP {} body={}",
                            this.name, this.partition, record.getOffset(), count,
                            response.statusCode(), response.body());
                    return;

                } catch (IOException e) {
                    if (attempt < maxAttempts && !this.stopped) {
                        long delay = computeBackoffDelay(this.config.backoff, this.config.delay,
                                this.config.maxDelay, attempt);
                        log.warn("[{}] partition={} offset={} count={} IO error (attempt {}/{}), retry in {}ms: {}",
                                this.name, this.partition, record.getOffset(), count,
                                attempt, maxAttempts, delay, e.getMessage());
                        LockSupport.parkNanos(delay * 1_000_000L);
                        continue;
                    }
                    log.error("[{}] partition={} offset={} count={} delivery failed after {} attempts: {}",
                            this.name, this.partition, record.getOffset(), count, maxAttempts, e.getMessage());
                    return;

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("[{}] partition={} offset={} count={} interrupted",
                            this.name, this.partition, record.getOffset(), count);
                    return;
                }
            }
        }
    }

}

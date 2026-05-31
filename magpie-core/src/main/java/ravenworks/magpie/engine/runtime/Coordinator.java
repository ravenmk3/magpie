package ravenworks.magpie.engine.runtime;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.engine.lock.LeaderLock;
import ravenworks.magpie.engine.sink.SinkConnector;
import ravenworks.magpie.engine.sink.SinkFactory;
import ravenworks.magpie.engine.sink.SinkRegistry;
import ravenworks.magpie.engine.source.SourceConnector;
import ravenworks.magpie.engine.source.SourceFactory;
import ravenworks.magpie.engine.source.SourceRegistry;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
@Slf4j
public class Coordinator {

    private static final Object WAKEUP_SIGNAL = new Object();
    private static final int DEFAULT_IDLE_TIMEOUT_MS = 5_000;

    private final EventLoop eventLoop;
    private final LeaderLock leaderLock;
    private final StreamRegistry streamRegistry;
    private final StreamProvider streamProvider;
    private final SourceRegistry sourceRegistry;
    private final SourceFactory sourceFactory;
    private final SinkRegistry sinkRegistry;
    private final SinkFactory sinkFactory;
    private final Map<String, SourceConnector> sourceConnectors = new LinkedHashMap<>();
    private final Map<String, SinkConnector> sinkConnectors = new LinkedHashMap<>();

    public Coordinator(@NonNull LeaderLock leaderLock,
                       @NonNull StreamRegistry streamRegistry,
                       @NonNull StreamProvider streamProvider,
                       @NonNull SourceRegistry sourceRegistry,
                       @NonNull SourceFactory sourceFactory,
                       @NonNull SinkRegistry sinkRegistry,
                       @NonNull SinkFactory sinkFactory) {
        this(leaderLock, streamRegistry, streamProvider,
                sourceRegistry, sourceFactory, sinkRegistry, sinkFactory, DEFAULT_IDLE_TIMEOUT_MS);
    }

    public Coordinator(@NonNull LeaderLock leaderLock,
                       @NonNull StreamRegistry streamRegistry,
                       @NonNull StreamProvider streamProvider,
                       @NonNull SourceRegistry sourceRegistry,
                       @NonNull SourceFactory sourceFactory,
                       @NonNull SinkRegistry sinkRegistry,
                       @NonNull SinkFactory sinkFactory,
                       int idleTimeoutMs) {
        this.leaderLock = leaderLock;
        this.streamRegistry = streamRegistry;
        this.streamProvider = streamProvider;
        this.sourceRegistry = sourceRegistry;
        this.sourceFactory = sourceFactory;
        this.sinkRegistry = sinkRegistry;
        this.sinkFactory = sinkFactory;
        this.eventLoop = new EventLoop("Coordinator", idleTimeoutMs, this::dispatch);
    }

    public void start() {
        this.eventLoop.start();
    }

    public CompletableFuture<Void> shutdown() {
        return this.eventLoop.shutdown();
    }

    public void wake() {
        this.eventLoop.enqueue(WAKEUP_SIGNAL);
    }

    private void dispatch(Object event) {
        if (event == WAKEUP_SIGNAL) {
            this.onWakeup();
            return;
        }
        switch (event) {
            case EventLoop.Idle _ -> this.onIdle();
            case EventLoop.Started _ -> this.onStarted();
            case EventLoop.PreShutdown _ -> this.onPreShutdown();
            case EventLoop.Terminated _ -> this.onTerminated();
            default -> log.warn("Unhandled event: {}", event);
        }
    }

    private void onWakeup() {
        this.coordinate();
    }

    private void onIdle() {
        this.coordinate();
    }

    private void onStarted() {
        this.leaderLock.init();
    }

    private void onTerminated() {
        this.leaderLock.release();
    }

    private void coordinate() {
        LeaderLock.PulseResult pr = this.leaderLock.pulse();
        switch (pr) {
            case ACQUIRED -> {
                log.info("Leader elected");
                this.onLeaderAcquired();
            }
            case RENEWED -> this.onLeaderRenewed();
            case LOST -> {
                log.warn("Leader lost");
                this.onLeaderLost();
            }
            case FAILED -> { /* not the leader, nothing to do */ }
        }
    }

    protected void onLeaderAcquired() {
        this.initStreams();
        this.startSourceConnectors();
        this.startSinkConnectors();
    }

    protected void onLeaderRenewed() {
    }

    protected void onLeaderLost() {
        this.shutdownSourceConnectors();
        this.shutdownSinkConnectors();
    }

    protected void onPreShutdown() {
        this.shutdownSourceConnectors();
        this.shutdownSinkConnectors();
    }

    private void initStreams() {
        var streams = this.streamRegistry.getStreams();
        for (var stream : streams) {
            this.streamProvider.create(stream);
        }
        log.info("Stream initialization complete, {} stream(s)", streams.size());
    }

    private void startSourceConnectors() {
        var sources = this.sourceRegistry.getSources();
        for (var definition : sources) {
            var connector = this.sourceFactory.create(definition);
            this.sourceConnectors.put(definition.getName(), connector);
            connector.start();
        }
        log.info("Source initialization complete, {} source(s)", sources.size());
    }

    private void shutdownSourceConnectors() {
        if (this.sourceConnectors.isEmpty()) {
            return;
        }
        var futures = this.sourceConnectors.values()
                .stream()
                .map(SourceConnector::shutdown)
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();
        this.sourceConnectors.clear();
        log.info("Source connectors shutdown complete");
    }

    private void startSinkConnectors() {
        var sinks = this.sinkRegistry.getSinks();
        for (var definition : sinks) {
            var connector = this.sinkFactory.create(definition);
            this.sinkConnectors.put(definition.getName(), connector);
            connector.start();
        }
        log.info("Sink initialization complete, {} sink(s)", sinks.size());
    }

    private void shutdownSinkConnectors() {
        if (this.sinkConnectors.isEmpty()) {
            return;
        }
        var futures = this.sinkConnectors.values()
                .stream()
                .map(SinkConnector::shutdown)
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();
        this.sinkConnectors.clear();
        log.info("Sink connectors shutdown complete");
    }

}

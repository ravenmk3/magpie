package ravenworks.magpie.engine.runtime;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.engine.lock.LeaderLock;
import ravenworks.magpie.engine.model.StreamDefinition;
import ravenworks.magpie.engine.store.MetaStore;
import ravenworks.magpie.engine.stream.StreamProvider;

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
    private final MetaStore metaStore;
    private final StreamProvider streamProvider;

    public Coordinator(@NonNull LeaderLock leaderLock,
                       @NonNull MetaStore metaStore,
                       @NonNull StreamProvider streamProvider) {
        this(leaderLock, metaStore, streamProvider, DEFAULT_IDLE_TIMEOUT_MS);
    }

    public Coordinator(@NonNull LeaderLock leaderLock,
                       @NonNull MetaStore metaStore,
                       @NonNull StreamProvider streamProvider,
                       int idleTimeoutMs) {
        this.leaderLock = leaderLock;
        this.metaStore = metaStore;
        this.streamProvider = streamProvider;
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
        var topics = this.metaStore.getTopics();
        for (var topic : topics) {
            this.streamProvider.create(new StreamDefinition(
                    topic.name(), topic.partitions(), topic.properties()));
        }
        log.info("Streaming initialization complete, {} topics", topics.size());
    }

    protected void onLeaderRenewed() {
    }

    protected void onLeaderLost() {
    }

    protected void onPreShutdown() {
    }

}

package ravenworks.magpie.engine.runtime;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.engine.lock.LeaderLock;

import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
@Slf4j
public class Coordinator {

    private static final Object WAKEUP_SIGNAL = new Object();
    private static final int DEFAULT_IDLE_TIMEOUT_MS = 10_000;

    private final EventLoop eventLoop;
    private final LeaderLock leaderLock;

    public Coordinator(@NonNull LeaderLock leaderLock) {
        this(leaderLock, DEFAULT_IDLE_TIMEOUT_MS);
    }

    public Coordinator(@NonNull LeaderLock leaderLock, int idleTimeoutMs) {
        this.leaderLock = leaderLock;
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
    }

    protected void onLeaderRenewed() {
    }

    protected void onLeaderLost() {
    }

    protected void onPreShutdown() {
    }

}

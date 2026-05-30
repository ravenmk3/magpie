package ravenworks.magpie.engine.lock;


/**
 * @author Raven
 */
public interface LeaderLock {

    void init();

    PulseResult pulse();

    void release();


    enum PulseResult {
        ACQUIRED,
        RENEWED,
        LOST,
        FAILED
    }

}

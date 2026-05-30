package ravenworks.magpie.server.config;

import lombok.NonNull;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ravenworks.magpie.domain.repository.LeaderLockRepository;
import ravenworks.magpie.domain.repository.TopicRepository;
import ravenworks.magpie.engine.lock.LeaderLock;
import ravenworks.magpie.engine.lock.LeaderLockImpl;
import ravenworks.magpie.engine.runtime.Coordinator;
import ravenworks.magpie.engine.store.MetaStore;
import ravenworks.magpie.engine.store.MetaStoreImpl;
import ravenworks.magpie.engine.stream.StreamProvider;


@Configuration
public class EngineConfiguration {

    @Bean
    public static LeaderLock leaderLock(@NonNull LeaderLockRepository lockRepository) {
        return new LeaderLockImpl(lockRepository);
    }

    @Bean
    public static MetaStore metaStore(@NonNull TopicRepository topicRepository) {
        return new MetaStoreImpl(topicRepository);
    }

    @Bean
    public static Coordinator coordinator(@NonNull LeaderLock leaderLock,
                                          @NonNull MetaStore metaStore,
                                          @NonNull StreamProvider streamProvider) {
        return new Coordinator(leaderLock, metaStore, streamProvider);
    }

    @Bean
    public static SmartLifecycle coordinatorLifecycle(@NonNull Coordinator coordinator) {
        return new SmartLifecycle() {

            @Override
            public void start() {
                coordinator.start();
            }

            @Override
            public void stop() {
                coordinator.shutdown().join();
            }

            @Override
            public boolean isRunning() {
                return false;
            }
        };
    }

}

package ravenworks.magpie.server.config;

import lombok.NonNull;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ravenworks.magpie.domain.repository.LeaderLockRepository;
import ravenworks.magpie.domain.repository.SinkRepository;
import ravenworks.magpie.domain.repository.SourceRepository;
import ravenworks.magpie.domain.repository.TopicRepository;
import ravenworks.magpie.engine.lock.LeaderLock;
import ravenworks.magpie.engine.lock.LeaderLockImpl;
import ravenworks.magpie.engine.runtime.Coordinator;
import ravenworks.magpie.engine.sink.SinkFactory;
import ravenworks.magpie.engine.sink.SinkFactoryImpl;
import ravenworks.magpie.engine.sink.SinkProvider;
import ravenworks.magpie.engine.sink.SinkRegistry;
import ravenworks.magpie.engine.source.SourceFactory;
import ravenworks.magpie.engine.source.SourceFactoryImpl;
import ravenworks.magpie.engine.source.SourceProvider;
import ravenworks.magpie.engine.source.SourceRegistry;
import ravenworks.magpie.engine.source.sample.SampleSourceProvider;
import ravenworks.magpie.engine.store.*;
import ravenworks.magpie.engine.stream.RoutingStreamProducer;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;

import java.util.ArrayList;
import java.util.List;


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
    public static StreamRegistry streamRegistry(@NonNull TopicRepository topicRepository) {
        return new StreamRegistryImpl(topicRepository);
    }

    @Bean
    public static SourceRegistry sourceRegistry(@NonNull SourceRepository sourceRepository) {
        return new SourceRegistryImpl(sourceRepository);
    }

    @Bean
    public static SourceFactory sourceFactory(@NonNull List<SourceProvider> providers) {
        var merged = new ArrayList<>(providers);
        merged.add(new SampleSourceProvider());
        return new SourceFactoryImpl(merged);
    }

    @Bean
    public static SinkRegistry sinkRegistry(@NonNull SinkRepository sinkRepository) {
        return new SinkRegistryImpl(sinkRepository);
    }

    @Bean
    public static SinkFactory sinkFactory(@NonNull List<SinkProvider> providers) {
        return new SinkFactoryImpl(providers);
    }

    @Bean
    public static Coordinator coordinator(@NonNull LeaderLock leaderLock,
                                          @NonNull StreamRegistry streamRegistry,
                                          @NonNull StreamProvider streamProvider,
                                          @NonNull SourceRegistry sourceRegistry,
                                          @NonNull SourceFactory sourceFactory,
                                          @NonNull SinkRegistry sinkRegistry,
                                          @NonNull SinkFactory sinkFactory,
                                          @NonNull RoutingStreamProducer streamProducer) {
        return new Coordinator(leaderLock, streamRegistry, streamProvider,
                sourceRegistry, sourceFactory, sinkRegistry, sinkFactory, streamProducer);
    }

    @Bean
    public static RoutingStreamProducer routingStreamProducer(@NonNull StreamProvider streamProvider,
                                                              @NonNull StreamRegistry streamRegistry) {
        return new RoutingStreamProducer(streamProvider, streamRegistry);
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

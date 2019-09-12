package org.apache.kafka.jmh.server;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import kafka.server.MetadataCache;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import scala.collection.JavaConverters;
import scala.collection.Seq;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MetadataCacheBenchmark {

    @State(Scope.Thread)
    public static class BenchmarkSupport {
        final int id = 1;
        final int controller = 2;
        final int controllerEpoch = 1;
        final int brokerEpoch = 1;
        final int leader = 3;
        final int leaderEpoch = 2;
        final MetadataCache cache = new MetadataCache(id);
        final int numTopics = 100;
        final int numPartitions = 1000;
        final int numBrokers = 10;
        final int querySize = 100;
        final SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        final ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        final Random random = new Random(0);

        scala.collection.Set<String> topics;

        @Setup
        public void setup() {
            Set<String> topics = new HashSet<>();
            Map<TopicPartition, UpdateMetadataRequest.PartitionState> partitions = new HashMap<>();

            for (int i = 0; i < querySize; ++i) {
                topics.add("topic-" + random.nextInt(numTopics));
            }
            this.topics = JavaConverters.asScalaSet(topics);

            for (int topicIndex = 0; topicIndex < numTopics; ++topicIndex) {
                for (int partition = 0; partition < numPartitions; ++partition) {

                    TopicPartition tp = new TopicPartition("topic-" + topicIndex, partition);
                    UpdateMetadataRequest.PartitionState state = new UpdateMetadataRequest.PartitionState(
                            controllerEpoch,
                            leader,
                            leaderEpoch,
                            Arrays.asList(id, leader),
                            0,
                            Arrays.asList(id, leader),
                            Collections.emptyList());

                    partitions.put(tp, state);
                }
            }

            Set<UpdateMetadataRequest.Broker> brokers = new HashSet<>();
            for (int broker = 0; broker < numBrokers; ++broker) {
                List<UpdateMetadataRequest.EndPoint> endpoints = Arrays.asList(new UpdateMetadataRequest.EndPoint(
                    "localhost", 9092, securityProtocol, listenerName));
                brokers.add(new UpdateMetadataRequest.Broker(broker, endpoints, null));
            }

            UpdateMetadataRequest request = new UpdateMetadataRequest.Builder((short) 5,
                    controller,
                    controllerEpoch,
                    brokerEpoch,
                    partitions,
                    brokers).build();
            cache.updateMetadata(5, request);
        }

    }

    @Benchmark
    public Seq<MetadataResponse.TopicMetadata> getTopicMetadata(BenchmarkSupport support) {
        return support.cache.getTopicMetadata(support.topics, support.listenerName, false, false);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MetadataCacheBenchmark.class.getCanonicalName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}


package com.github.charithe.kafka;

import kafka.server.KafkaConfig;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EphemeralKafkaCluster implements AutoCloseable {
    private static final int ALLOCATE_RANDOM_PORT = -1;

    private int numBroker;
    private TestingServer zookeeper;
    private final List<EphemeralKafkaBroker> brokers = new ArrayList<>();

    private EphemeralKafkaCluster(int numBroker, int zookeeperPort, Properties brokerProperties) throws Exception {
        this.zookeeper = new TestingServer(zookeeperPort);
        this.numBroker = numBroker;
        for (int i = 0; i< numBroker; ++i) {
            this.addBroker(brokerProperties);
        }
    }

    /**
     * Create a new ephemeral Kafka cluster
     *
     * @param numBroker number of brokers
     * @return EphemeralKafkaCluster
     * @throws Exception if create fails
     */
    public static EphemeralKafkaCluster create(int numBroker) throws Exception {
        return create(numBroker, ALLOCATE_RANDOM_PORT);
    }

    /**
     * Create a new ephemeral Kafka cluster with the specified Zookeeper port
     *
     * @param numBroker Number of brokers
     * @param zookeeperPort Port the Zookeeper should listen on
     * @return EphemeralKafkaCluster
     * @throws Exception if create fails
     */
    public static EphemeralKafkaCluster create(int numBroker, int zookeeperPort) throws Exception {
        return create(numBroker, zookeeperPort, new Properties());
    }

    /**
     * Create a new ephemeral Kafka cluster with the specified Zookeeper port and broker properties
     *
     * @param numBroker Number of brokers
     * @param zookeeperPort Port the Zookeeper should listen on
     * @param brokerProperties Override properties for all brokers in the cluster
     * @return EphemeralKafkaCluster
     * @throws Exception if create fails
     */
    public static EphemeralKafkaCluster create(int numBroker, int zookeeperPort, Properties brokerProperties) throws Exception {
        return new EphemeralKafkaCluster(numBroker, zookeeperPort, brokerProperties);
    }

    public boolean isHealthy() {
        return brokers.stream().filter(b -> !b.isRunning()).count() == 0;
    }

    public boolean isRunning() {
        return brokers.stream().filter(EphemeralKafkaBroker::isRunning).count() > 0;
    }

    public void stop() throws IOException, ExecutionException, InterruptedException {
        CompletableFuture.allOf(brokers.stream().map(EphemeralKafkaBroker::stopBrokerAsync).toArray(CompletableFuture[]::new)).get();
        brokers.clear();
        zookeeper.stop();
    }

    private EphemeralKafkaBroker addBroker(Properties overrideBrokerProperties) throws Exception {
        final int brokerPort = InstanceSpec.getRandomPort();
        Properties brokerConfigProperties = new Properties();
        brokerConfigProperties.setProperty(KafkaConfig.BrokerIdProp(), brokers.size() + "");
        brokerConfigProperties.setProperty(KafkaConfig.ZkConnectProp(), zookeeper.getConnectString());
        brokerConfigProperties.setProperty(KafkaConfig.ControlledShutdownEnableProp(), false + "");
        brokerConfigProperties.setProperty(KafkaConfig.ControlledShutdownMaxRetriesProp(), "1");
        brokerConfigProperties.setProperty(KafkaConfig.DeleteTopicEnableProp(), true + "");
        brokerConfigProperties.setProperty(KafkaConfig.SslEnabledProtocolsProp(), false + "");
        brokerConfigProperties.setProperty(KafkaConfig.AutoCreateTopicsEnableProp(), true + "");
        brokerConfigProperties.setProperty(KafkaConfig.ReplicaSocketTimeoutMsProp(), "300");
        brokerConfigProperties.setProperty(KafkaConfig.ReplicaFetchWaitMaxMsProp(), "100");
        brokerConfigProperties.setProperty(KafkaConfig.ControllerSocketTimeoutMsProp(), "10");

        brokerConfigProperties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), numBroker + "");
        brokerConfigProperties.setProperty(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp(), 1 + "");
        brokerConfigProperties.setProperty(KafkaConfig.ZkSessionTimeoutMsProp(), 200 + "");
        brokerConfigProperties.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsDoc(), 200 + "");
        brokerConfigProperties.setProperty(KafkaConfig.AdvertisedListenersProp(), "PLAINTEXT://localhost:" + brokerPort);
        brokerConfigProperties.setProperty(KafkaConfig.MinInSyncReplicasProp(), Math.max(1, numBroker - 1) + "");
        if(!overrideBrokerProperties.isEmpty()){
            brokerConfigProperties.putAll(overrideBrokerProperties);
        }
        final EphemeralKafkaBroker broker = new EphemeralKafkaBroker(zookeeper, brokerPort, brokerConfigProperties);
        broker.start().get();
        brokers.add(broker);
        return broker;
    }

    public List<EphemeralKafkaBroker> getBrokers() {
        return Collections.unmodifiableList(brokers);
    }

    public String connectionString() {
        return brokers.stream().filter(EphemeralKafkaBroker::isRunning).map(EphemeralKafkaBroker::getBrokerList).map(Optional::get).collect(Collectors.joining(","));
    }

    /**
     * Create a minimal producer configuration that can be used to produce to this broker
     *
     * @return Properties
     */
    public Properties producerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", connectionString());
        props.put("acks", "all");
        props.put("batch.size", "100");
        props.put("client.id", "kafka-junit");
        props.put("request.timeout.ms", "5000");
        props.put("max.in.flight.requests.per.connection", "1");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        return props;
    }

    /**
     * Create a minimal consumer configuration with auto commit enabled. Offset is set to "earliest".
     *
     * @return Properies
     */
    public Properties consumerConfig() {
        return consumerConfig(true);
    }

    /**
     * Create a minimal consumer configuration. Offset is set to "earliest".
     *
     * @param enableAutoCommit Enable auto commit
     * @return Properties
     */
    public Properties consumerConfig(boolean enableAutoCommit) {
        Properties props = new Properties();
        props.put("bootstrap.servers", connectionString());
        props.put("group.id", "kafka-junit-consumer");
        props.put("enable.auto.commit", String.valueOf(enableAutoCommit));
        props.put("auto.commit.interval.ms", "100");
        props.put("auto.offset.reset", "earliest");
        props.put("heartbeat.interval.ms", "100");
        props.put("session.timeout.ms", "200");
        props.put("fetch.max.wait.ms", "500");
        props.put("metadata.max.age.ms", "100");
        return props;
    }

    public void createTopics(String... topics) throws ExecutionException, InterruptedException {
        Map<String, Object> adminConfigs = new HashMap<>();
        adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connectionString());
        try(AdminClient admin = AdminClient.create(adminConfigs)) {
            List<NewTopic> newTopics = Stream.of(topics)
                                             .map(t -> new NewTopic(t, numBroker, (short) numBroker))
                                             .collect(Collectors.toList());
            CreateTopicsResult createTopics = admin.createTopics(newTopics);
            createTopics.all().get();
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}

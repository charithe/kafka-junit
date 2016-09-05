/*
 * Copyright 2016 Charith Ellawala
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.charithe.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;

public class KafkaJunitRule extends ExternalResource {

    private static final int POLL_TIMEOUT_MS = 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJunitRule.class);
    private static final int ALLOCATE_RANDOM_PORT = -1;
    private static final String LOCALHOST = "localhost";
    private Properties brokerProperties = null;
    private Properties consumerProperties = null;

    private TestingServer zookeeper;
    private KafkaServerStartable kafkaServer;

    private int zookeeperPort = ALLOCATE_RANDOM_PORT;
    private String zookeeperConnectionString;
    private int kafkaPort;
    private Path kafkaLogDir;

    public KafkaJunitRule() {
        this(ALLOCATE_RANDOM_PORT);
    }

    public KafkaJunitRule(final int kafkaPort) {
        if (kafkaPort == ALLOCATE_RANDOM_PORT) {
            this.kafkaPort = InstanceSpec.getRandomPort();
        } else {
            this.kafkaPort = kafkaPort;
        }
    }

    public KafkaJunitRule(final int kafkaPort, final int zookeeperPort) {
        this(kafkaPort);
        this.zookeeperPort = zookeeperPort;
    }

    public KafkaJunitRule(final int kafkaPort, final int zookeeperPort, Properties brokerProperties) {
        this(kafkaPort, zookeeperPort);
        this.brokerProperties = brokerProperties;
    }

    public KafkaJunitRule(final int kafkaPort, final int zookeeperPort, Properties brokerProperties,
                          Properties consumerProperties) {
        this(kafkaPort, zookeeperPort);
        this.brokerProperties = brokerProperties;
        this.consumerProperties = consumerProperties;
    }

    @Override
    protected void before() throws Throwable {
        if (zookeeperPort == ALLOCATE_RANDOM_PORT) {
            zookeeper = new TestingServer(true);
            zookeeperPort = zookeeper.getPort();
        } else {
            zookeeper = new TestingServer(zookeeperPort, true);
        }
        zookeeperConnectionString = zookeeper.getConnectString();
        KafkaConfig kafkaConfig = buildKafkaConfig(zookeeperConnectionString);

        LOGGER.info("Starting Kafka server with config: {}", kafkaConfig.props());
        kafkaServer = new KafkaServerStartable(kafkaConfig);
        startKafka();
    }

    @Override
    protected void after() {
        try {
            shutdownKafka();

            if (zookeeper != null) {
                LOGGER.info("Shutting down Zookeeper");
                zookeeper.close();
            }

            if (Files.exists(kafkaLogDir)) {
                LOGGER.info("Deleting the log dir:  {}", kafkaLogDir);
                Files.walkFileTree(kafkaLogDir, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.deleteIfExists(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.deleteIfExists(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.error("Failed to clean-up Kafka", e);
        }
    }

    /**
     * Shutdown Kafka Broker before the test termination to test consumer exceptions
     */
    public void shutdownKafka() {
        if (kafkaServer != null) {
            LOGGER.info("Shutting down Kafka Server");
            kafkaServer.shutdown();
        }
    }

    /**
     * Starts the server
     */
    public void startKafka() {
        if (kafkaServer != null) {
            LOGGER.info("Starting Kafka Server");
            kafkaServer.startup();
        }
    }

    private KafkaConfig buildKafkaConfig(String zookeeperQuorum) throws IOException {
        kafkaLogDir = Files.createTempDirectory("kafka_junit");

        Properties props = new Properties();
        props.put("advertised.host.name", LOCALHOST);
        props.put("port", kafkaPort + "");
        props.put("broker.id", "1");
        props.put("log.dirs", kafkaLogDir.toAbsolutePath().toString());
        props.put("zookeeper.connect", zookeeperQuorum);
        props.put("leader.imbalance.check.interval.seconds", "1");
        props.put("offsets.topic.num.partitions", "1");
        props.put("offsets.topic.replication.factor", "1");
        props.put("default.replication.factor", "1");
        props.put("group.min.session.timeout.ms", "100");

        if (brokerProperties != null) {
            props.putAll(brokerProperties);
        }

        return new KafkaConfig(props);
    }

    /**
     * Create a producer configuration.
     */
    public Properties producerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", LOCALHOST + ":" + kafkaPort);
        props.put("acks", "1");
        props.put("request.timeout.ms", "500");

        return props;
    }

    /**
     * Create a consumer configuration. Auto commit is enabled.
     *
     * @return
     */
    public Properties consumerConfig() {
        return consumerConfig(true);
    }

    /**
     * Create a consumer configuration. Offset is set to "earliest".
     */
    public Properties consumerConfig(boolean enableAutoCommit) {
        Properties props = new Properties();
        props.put("bootstrap.servers", LOCALHOST + ":" + kafkaPort);
        props.put("group.id", "kafka-junit-consumer");
        props.put("enable.auto.commit", String.valueOf(enableAutoCommit));
        props.put("auto.commit.interval.ms", "10");
        props.put("auto.offset.reset", "earliest");
        props.put("heartbeat.interval.ms", "100");
        props.put("session.timeout.ms", "200");
        props.put("fetch.max.wait.ms", "200");

        if (consumerProperties != null) {
            props.putAll(consumerProperties);
        }

        return props;
    }

    /**
     * Create a Kafka consumer that reads messages with String key and values. Auto commit is enabled for the consumer.
     *
     * @return KafkaConsumer
     */
    public KafkaConsumer<String, String> createStringConsumer() {
        return createConsumer(new StringDeserializer(), new StringDeserializer());
    }

    /**
     * Create a Kafka consumer that reads messages with String key and values.
     *
     * @param autoCommitEnabled Set to true to enable auto commit
     * @return KafkaConsumer
     */
    public KafkaConsumer<String, String> createStringConsumer(boolean autoCommitEnabled) {
        return createConsumer(new StringDeserializer(), new StringDeserializer(), autoCommitEnabled);
    }

    /**
     * Create a Kafka consumer using {@link #consumerConfig()}
     *
     * @return KafkaConsumer
     */
    public <K, V> KafkaConsumer<K, V> createConsumer(Deserializer<K> keyDeserializer,
                                                     Deserializer<V> valueDeserializer) {
        return new KafkaConsumer<>(consumerConfig(), keyDeserializer, valueDeserializer);
    }

    /**
     * Create a Kafka consumer using {@link #consumerConfig(boolean)}
     *
     * @return KafkaConsumer
     */
    public <K, V> KafkaConsumer<K, V> createConsumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
                                                     boolean autoCommitEnabled) {
        return new KafkaConsumer<>(consumerConfig(autoCommitEnabled), keyDeserializer, valueDeserializer);
    }

    /**
     * Create a Kafka producer that produces messages with String keys and values
     */
    public KafkaProducer<String, String> createStringProducer() {
        return createProducer(new StringSerializer(), new StringSerializer());
    }

    /**
     * Create a Kafka producer using {@link #producerConfig()}
     *
     * @return KafkaProducer
     */
    public <K, V> KafkaProducer<K, V> createProducer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new KafkaProducer<>(producerConfig(), keySerializer, valueSerializer);
    }

    /**
     * @deprecated Use {@link #pollStringMessages(String, int)} instead
     * <p>
     * This method is deprecated because it silently creates a new consumer on each invocation -- which could lead to
     * some mesages being read more than once. It also suffers from the problems described in the deprecation notice for
     * {@link #readMessages(KafkaConsumer, String, int, int)} as well.
     */
    @Deprecated
    public List<String> readStringMessages(final String topic, final int expectedMessages, final int timeoutSeconds)
            throws TimeoutException {
        return readMessages(createStringConsumer(), topic, expectedMessages, timeoutSeconds);
    }

    /**
     * @deprecated Use {@link #pollMessages(String, int, Deserializer, Deserializer)} instead.
     * <p>
     * This method is deprecated due to the switch to Kafka 0.9. The new API can return more than the expected number
     * of messages unless manual offset management is performed. Since it is impossible to ensure that the passed-in
     * KafkaConsumer has auto commit disabled, this method should not be used.
     */
    @Deprecated
    public <T> List<T> readMessages(final KafkaConsumer<T, T> consumer, final String topic, final int expectedMessages,
                                    final int timeoutSeconds) throws TimeoutException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        try {
            consumer.subscribe(Lists.newArrayList(topic));
            Future<List<T>> future = singleThread.submit(new Callable<List<T>>() {
                @Override
                public List<T> call() throws Exception {
                    List<T> messages = new ArrayList<>(expectedMessages);
                    while (messages.size() < expectedMessages) {
                        ConsumerRecords<T, T> records = consumer.poll(POLL_TIMEOUT_MS);
                        for (ConsumerRecord<T, T> rec : records) {
                            LOGGER.debug("Received message: {} -> {}", rec.key(), rec.value());
                            messages.add(rec.value());
                        }
                    }
                    return messages;
                }
            });

            return future.get(timeoutSeconds, SECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            throw new TimeoutException("Timed out waiting for messages");
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception while reading messages", e);
        } finally {
            singleThread.shutdown();
        }
    }

    /**
     * Poll the specified topic for messages
     *
     * @param topic             Topic to poll
     * @param numMessagesToPoll Number of messages to read
     * @param keyDeserializer   Key deserializer
     * @param valueDeserializer Value deserializer
     * @param <K>               Type of Key
     * @param <V>               Type of Value
     * @return {@link ListenableFuture} containing a list of {@link ConsumerRecord} objects
     */
    public <K, V> ListenableFuture<List<ConsumerRecord<K, V>>> pollMessages(String topic, int numMessagesToPoll,
                                                                            Deserializer<K> keyDeserializer,
                                                                            Deserializer<V> valueDeserializer) {

        final KafkaConsumer<K, V> consumer = createConsumer(keyDeserializer, valueDeserializer, false);
        consumer.subscribe(Lists.newArrayList(topic));
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        return executor.submit(new RecordConsumer<>(numMessagesToPoll, consumer));
    }

    /**
     * Poll the specified topic for String key-valued messages
     *
     * @param topic             Topic to poll
     * @param numMessagesToPoll Number of messages to read
     * @return {@link ListenableFuture} containing a list of {@link ConsumerRecord} objects
     */
    public ListenableFuture<List<ConsumerRecord<String, String>>> pollStringMessages(String topic, int numMessagesToPoll) {
        return pollMessages(topic, numMessagesToPoll, new StringDeserializer(), new StringDeserializer());
    }

    /**
     * Get the Kafka log directory
     *
     * @return kafka log directory path
     */
    public Path kafkaLogDir() {
        return kafkaLogDir;
    }

    /**
     * Get the kafka broker port
     *
     * @return broker port
     */
    public int kafkaBrokerPort() {
        return kafkaPort;
    }

    /**
     * Get the zookeeper port
     *
     * @return zookeeper port
     */
    public int zookeeperPort() {
        return zookeeperPort;
    }

    /**
     * Get the zookeeper connection string
     *
     * @return zookeeper connection string
     */
    public String zookeeperConnectionString() {
        return zookeeperConnectionString;
    }


    private static class RecordConsumer<K, V> implements Callable<List<ConsumerRecord<K, V>>> {
        private final int numRecordsToPoll;
        private final KafkaConsumer<K, V> consumer;

        RecordConsumer(int numRecordsToPoll, KafkaConsumer<K, V> consumer) {
            this.numRecordsToPoll = numRecordsToPoll;
            this.consumer = consumer;
        }

        @Override
        public List<ConsumerRecord<K, V>> call() throws Exception {
            try {
                Map<TopicPartition, OffsetAndMetadata> commitBuffer = Maps.newHashMap();
                List<ConsumerRecord<K, V>> polledMessages = new ArrayList<>(numRecordsToPoll);
                while ((polledMessages.size() < numRecordsToPoll) && (!Thread.currentThread().isInterrupted())) {
                    ConsumerRecords<K, V> records = consumer.poll(0);
                    for (ConsumerRecord<K, V> rec : records) {
                        LOGGER.debug("Received message: {} -> {}", rec.key(), rec.value());
                        polledMessages.add(rec);
                        commitBuffer.put(
                                new TopicPartition(rec.topic(), rec.partition()),
                                new OffsetAndMetadata(rec.offset() + 1)
                        );

                        if (polledMessages.size() == numRecordsToPoll) {
                            consumer.commitSync(commitBuffer);
                            break;
                        }
                    }
                }
                return polledMessages;
            } finally {
                consumer.close();
            }
        }
    }
}

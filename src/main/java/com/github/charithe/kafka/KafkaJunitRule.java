/*
 * Copyright 2015 Charith Ellawala
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

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.ProducerConfig;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
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

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Starts up a local Zookeeper and a Kafka broker
 */
public class KafkaJunitRule extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJunitRule.class);
    private static final int ALLOCATE_RANDOM_PORT = -1;
    private static final String LOCALHOST = "localhost";

    private TestingServer zookeeper;
    private KafkaServerStartable kafkaServer;

    private int zookeeperPort = ALLOCATE_RANDOM_PORT;
    private String zookeeperConnectionString;
    private int kafkaPort;
    private Path kafkaLogDir;

    public KafkaJunitRule() {
        this(ALLOCATE_RANDOM_PORT);
    }

    public KafkaJunitRule(final int kafkaPort, final int zookeeperPort){
        this(kafkaPort);
        this.zookeeperPort = zookeeperPort;
    }


    public KafkaJunitRule(final int kafkaPort) {
        if (kafkaPort == ALLOCATE_RANDOM_PORT) {
            this.kafkaPort = InstanceSpec.getRandomPort();
        } else {
            this.kafkaPort = kafkaPort;
        }
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
        }
        catch(Exception e){
            LOGGER.error("Failed to clean-up Kafka",e);
        }
    }

    /**
     *
     * Shutdown Kafka Broker before the test termination to test consumer exceptions
     *
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
    public void startKafka(){
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

        return new KafkaConfig(props);
    }

    /**
     * Create a producer configuration.
     * Sets the serializer class to "DefaultEncoder" and producer type to "sync"
     *
     * @return {@link ProducerConfig}
     */
    public ProducerConfig producerConfigWithDefaultEncoder() {
        return producerConfig("kafka.serializer.DefaultEncoder");
    }

    /**
     * Create a producer configuration.
     * Sets the serializer class to "StringEncoder" and producer type to "sync"
     *
     * @return {@link ProducerConfig}
     */
    public ProducerConfig producerConfigWithStringEncoder() {
        return producerConfig("kafka.serializer.StringEncoder");
    }

    /**
     * Create a producer configuration.
     * Sets the serializer class to specified encoder and producer type to "sync"
     *
     * @return {@link ProducerConfig}
     */
    public ProducerConfig producerConfig(String serializerClass) {
        Properties props = new Properties();
        props.put("external.bootstrap.servers", LOCALHOST + ":" + kafkaPort);
        props.put("metadata.broker.list", LOCALHOST + ":" + kafkaPort);
        props.put("serializer.class", serializerClass);
        props.put("producer.type", "sync");
        props.put("request.required.acks", "1");

        return new ProducerConfig(props);
    }

    /**
     * Create a consumer configuration
     * Offset is set to "smallest"
     * @return {@link ConsumerConfig}
     */
    public ConsumerConfig consumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperConnectionString);
        props.put("group.id", "kafka-junit-consumer");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }
    
    /**
     * Read messages from a given kafka topic as {@link String}.
     * @param topic name of the topic
     * @param expectedMessages number of messages to be read
     * @return list of string messages
     * @throws TimeoutException if no messages are read after 5 seconds
     */
    public List<String> readStringMessages(final String topic, final int expectedMessages) throws TimeoutException {
        return readMessages(topic, expectedMessages, new StringDecoder(null));
    }
    
    /**
     * Read messages from a given kafka topic.
     * @param topic name of the topic
     * @param expectedMessages number of messages to be read
     * @param decoder message decoder
     * @return list of decoded messages
     * @throws TimeoutException if no messages are read after 5 seconds
     */
    public <T> List<T> readMessages(final String topic, final int expectedMessages, final Decoder<T> decoder) throws TimeoutException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        ConsumerConnector connector = null;
        try {
            connector = Consumer.createJavaConsumerConnector(consumerConfig());

            Map<String, List<KafkaStream<byte[], T>>> streams = connector.createMessageStreams(
                    singletonMap(topic, 1), new DefaultDecoder(null), decoder);
            
            final KafkaStream<byte[], T> messageSteam = streams.get(topic).get(0);
            
            Future<List<T>> future = singleThread.submit(new Callable<List<T>>() {
                @Override
                public List<T> call() throws Exception {
                    List<T> messages = new ArrayList<>(expectedMessages);
                    ConsumerIterator<byte[], T> iterator = messageSteam.iterator();
                    while (messages.size() != expectedMessages && iterator.hasNext()) {
                        T message = iterator.next().message();
                        LOGGER.debug("Received message: {}", message);
                        messages.add(message);
                    }
                    return messages;
                }
            });
            
            return future.get(5, SECONDS);
            
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            throw new TimeoutException("Timed out waiting for messages");
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception while reading messages", e);
        } finally {
            singleThread.shutdown();
            if (connector != null) {
                connector.shutdown();
            }
        }
    }

    /**
     * Get the Kafka log directory
     * @return kafka log directory path
     */
    public Path kafkaLogDir(){
        return kafkaLogDir;
    }

    /**
     * Get the kafka broker port
     * @return broker port
     */
    public int kafkaBrokerPort(){
        return kafkaPort;
    }

    /**
     * Get the zookeeper port
     * @return zookeeper port
     */
    public int zookeeperPort(){
        return zookeeperPort;
    }

    /**
     * Get the zookeeper connection string
     * @return zookeeper connection string
     */
    public String zookeeperConnectionString(){
        return zookeeperConnectionString;
    }
}

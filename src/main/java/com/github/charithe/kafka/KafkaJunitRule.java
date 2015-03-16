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

import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;
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
import java.util.Properties;

/**
 * Starts up a local Zookeeper and a Kafka broker
 */
public class KafkaJunitRule extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJunitRule.class);
    private static final int ALLOCATE_RANDOM_PORT = -1;

    private TestingServer zookeeper;
    private KafkaServerStartable kafkaServer;

    private int zookeeperPort;
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

    @Override
    protected void before() throws Throwable {
        zookeeper = new TestingServer(true);
        zookeeperPort = zookeeper.getPort();
        zookeeperConnectionString = zookeeper.getConnectString();
        KafkaConfig kafkaConfig = buildKafkaConfig(zookeeperConnectionString);

        LOGGER.info("Starting Kafka server with config: {}", kafkaConfig.props().props());
        kafkaServer = new KafkaServerStartable(kafkaConfig);
        kafkaServer.startup();
    }

    @Override
    protected void after() {
        try {
            if (kafkaServer != null) {
                LOGGER.info("Shutting down Kafka Server");
                kafkaServer.shutdown();
            }

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

    private KafkaConfig buildKafkaConfig(String zookeeperQuorum) throws IOException {
        kafkaLogDir = Files.createTempDirectory("kafka_junit");

        Properties props = new Properties();
        props.put("port", kafkaPort + "");
        props.put("broker.id", "1");
        props.put("log.dirs", kafkaLogDir.toAbsolutePath().toString());
        props.put("zookeeper.connect", zookeeperQuorum);


        return new KafkaConfig(props);
    }


    /**
     * Create a producer configuration.
     * Sets the serializer class to "StringEncoder" and producer type to "sync"
     *
     * @return {@link ProducerConfig}
     */
    public ProducerConfig producerConfig() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:" + kafkaPort);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
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

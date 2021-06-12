package com.github.charithe.kafka;

import com.google.common.collect.Lists;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

import static com.github.charithe.kafka.EphemeralKafkaBrokerTest.TEN_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaJunitExtensionTest {

    private static final String TOPIC = "topicX";

    @Nested
    @ExtendWith(KafkaJunitExtension.class)
    class BaseTest {

        @Test
        void testKafkaServerIsUp(KafkaHelper kafkaHelper) {
            try (KafkaProducer<String, String> producer = kafkaHelper.createStringProducer()) {
                producer.send(new ProducerRecord<>(TOPIC, "keyA", "valueA"));
            }

            try (KafkaConsumer<String, String> consumer = kafkaHelper.createStringConsumer()) {
                consumer.subscribe(Lists.newArrayList(TOPIC));
                ConsumerRecords<String, String> records = consumer.poll(TEN_SECONDS);
                Assertions.assertAll(() -> assertThat(records).isNotNull(),
                                     () -> assertThat(records.isEmpty()).isFalse());

                ConsumerRecord<String, String> msg = records.iterator().next();
                Assertions.assertAll(() -> assertThat(msg).isNotNull(),
                                     () -> assertThat(msg.key()).isEqualTo("keyA"),
                                     () -> assertThat(msg.value()).isEqualTo("valueA"));
            }
        }
    }

    @Nested
    @ExtendWith(KafkaJunitExtension.class)
    @KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
    class WaitForStartup {

        @Test
        void testKafkaServerIsUp(KafkaHelper kafkaHelper) {
            // Setup Zookeeper client
            final String zkConnectionString = kafkaHelper.zookeeperConnectionString();
            final ZooKeeperClient zooKeeperClient = new ZooKeeperClient(zkConnectionString, 2000, 8000, Integer.MAX_VALUE, Time.SYSTEM,"kafka.server", "SessionExpireListener" );
            final KafkaZkClient zkClient = new KafkaZkClient(zooKeeperClient, JaasUtils.isZkSaslEnabled(), Time.SYSTEM);
            final AdminZkClient adminZkClient = new AdminZkClient(zkClient);

            // Create topic
            adminZkClient.createTopic(TOPIC, 1, 1, new Properties(), null, false);

            // Produce/consume test
            try (KafkaProducer<String, String> producer = kafkaHelper.createStringProducer()) {
                producer.send(new ProducerRecord<>(TOPIC, "keyA", "valueA"));
            }

            try (KafkaConsumer<String, String> consumer = kafkaHelper.createStringConsumer()) {
                consumer.subscribe(Lists.newArrayList(TOPIC));
                ConsumerRecords<String, String> records = consumer.poll(10000);
                Assertions.assertAll(() -> assertThat(records).isNotNull(),
                                     () -> assertThat(records.isEmpty()).isFalse());

                ConsumerRecord<String, String> msg = records.iterator().next();
                Assertions.assertAll(() -> assertThat(msg).isNotNull(),
                                     () -> assertThat(msg.key()).isEqualTo("keyA"),
                                     () -> assertThat(msg.value()).isEqualTo("valueA"));
            }
        }

        @Test
        void brokerIsStarted(EphemeralKafkaBroker broker) {
            assertThat(broker.isRunning()).isTrue();
        }
    }
}
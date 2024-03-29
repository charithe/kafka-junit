/*
 * Copyright 2016 Charith Ellawala
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.charithe.kafka;

import com.google.common.collect.Lists;
import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class EphemeralKafkaBrokerTest {

    static final Duration TEN_SECONDS = Duration.ofSeconds(10);
    static final String TEST_TOPIC = "test-topic";

    @Test
    public void testStartAndStop() throws Exception {
        int kafkaPort = InstanceSpec.getRandomPort();
        int zkPort = InstanceSpec.getRandomPort();
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(kafkaPort, zkPort);

        assertThat(broker.start()).succeedsWithin(Duration.ofSeconds(3));

        assertThat(broker.isRunning()).isTrue();
        assertThat(broker.getKafkaPort()).hasValue(kafkaPort);
        assertThat(broker.getZookeeperPort()).hasValue(zkPort);
        assertThat(broker.getBrokerList()).isPresent();
        assertThat(broker.getZookeeperConnectString()).isPresent();
        assertThat(broker.getLogDir()).isPresent();

        Path logDir = Paths.get(broker.getLogDir().get());
        assertThat(logDir).exists();

        broker.stop();
        assertThat(broker.isRunning()).isFalse();
        assertThat(broker.getBrokerList()).isNotPresent();
        assertThat(broker.getZookeeperConnectString()).isNotPresent();
        assertThat(logDir).doesNotExist();
    }

    @Test
    public void testReadAndWrite() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create();
        assertThat(broker.start()).succeedsWithin(Duration.ofSeconds(3));

        assertThat(broker.isRunning()).isTrue();

        try (KafkaProducer<String, String> producer =
                     broker.createProducer(new StringSerializer(), new StringSerializer(), null)) {
            Future<RecordMetadata> result =
                    producer.send(new ProducerRecord<>(TEST_TOPIC, "key1", "value1"));

            RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);
        }

        try (KafkaConsumer<String, String> consumer =
                     broker.createConsumer(new StringDeserializer(), new StringDeserializer(), null)) {

            consumer.subscribe(Lists.newArrayList(TEST_TOPIC));
            ConsumerRecords<String, String> records;
            records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records).isNotNull();
            assertThat(records).isNotEmpty();

            ConsumerRecord<String, String> msg = records.iterator().next();
            assertThat(msg).isNotNull();
            assertThat(msg.key()).isEqualTo("key1");
            assertThat(msg.value()).isEqualTo("value1");
        }

        broker.stop();
    }
}

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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static com.github.charithe.kafka.EphemeralKafkaBrokerTest.TEN_SECONDS;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class EphemeralKafkaClusterTest {

    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_TOPIC2 = "test-topic2";
    private static EphemeralKafkaCluster cluster;

    @BeforeClass
    public static void beforeClass() throws Exception {
        cluster = EphemeralKafkaCluster.create(3);
    }

    @AfterClass
    public static void afterClass() throws InterruptedException, ExecutionException, IOException {
        cluster.stop();
    }

    @Test
    public void testStartAndStop() throws Exception {
        try (KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(cluster.consumerConfig(false), new IntegerDeserializer(), new StringDeserializer());
             KafkaProducer<Integer, String> producer = new KafkaProducer<>(cluster.producerConfig(), new IntegerSerializer(), new StringSerializer())) {
            cluster.createTopics(TEST_TOPIC);

            producer.send(new ProducerRecord<>(TEST_TOPIC, "value"));
            producer.flush();

            consumer.subscribe(Collections.singleton(TEST_TOPIC));
            ConsumerRecords<Integer, String> poll = consumer.poll(TEN_SECONDS);
            assertThat(poll.count()).isEqualTo(1);
            assertThat(poll.iterator().next().value()).isEqualTo("value");
        }
    }

    @Test
    public void testBrokerFailure() throws Exception {
        try (KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(cluster.consumerConfig(false), new IntegerDeserializer(), new StringDeserializer());
             KafkaProducer<Integer, String> producer = new KafkaProducer<>(cluster.producerConfig(), new IntegerSerializer(), new StringSerializer())) {
            cluster.createTopics(TEST_TOPIC2);

            producer.send(new ProducerRecord<>(TEST_TOPIC2, 1, "value"));
            producer.send(new ProducerRecord<>(TEST_TOPIC2, 2, "value"));
            producer.send(new ProducerRecord<>(TEST_TOPIC2, 3, "value"));
            producer.flush();

            consumer.subscribe(Collections.singleton(TEST_TOPIC2));
            int count = 0;
            for(int i = 0 ; i < 10 ; ++i) {
                ConsumerRecords<Integer, String> poll = consumer.poll(TEN_SECONDS);
                count += poll.count();
                if(count == 3)
                    break;
            }
            assertThat(count).isEqualTo(3);
        }
        cluster.getBrokers().get(1).stop();
        try (KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(cluster.consumerConfig(false), new IntegerDeserializer(), new StringDeserializer());
             KafkaProducer<Integer, String> producer = new KafkaProducer<>(cluster.producerConfig(), new IntegerSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(TEST_TOPIC2, 1, "value"));
            producer.send(new ProducerRecord<>(TEST_TOPIC2, 2, "value"));
            producer.send(new ProducerRecord<>(TEST_TOPIC2, 3, "value"));
            producer.flush();

            consumer.subscribe(Collections.singleton(TEST_TOPIC2));
            int count = 0;
            for(int i = 0 ; i < 10 ; ++i) {
                ConsumerRecords<Integer, String> poll = consumer.poll(TEN_SECONDS);
                count += poll.count();
                if(count == 6)
                    break;
            }
            assertThat(count).isEqualTo(6);
        }
        cluster.getBrokers().get(1).start().get();
    }
}

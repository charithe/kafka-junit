/*
 * Copyright 2016 Janne K. Olesen
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
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ExternalZookeeperTest {

    private static final String TOPIC = "topicX";
    private static final String KEY_1 = "keyX1";
    private static final String VALUE_1 = "valueX1";

    private static TestingServer zookeeper;

    static {
        try {
            zookeeper = new TestingServer();
        } catch (Exception e) {
            fail("zookeeper setup failed");
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        zookeeper.close();
    }

    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule(zookeeper);

    @Test
    public void testKafkaServerIsUp() {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY_1, VALUE_1));
        }

        try (KafkaConsumer<String, String> consumer = kafkaRule.createStringConsumer()) {
            consumer.subscribe(Lists.newArrayList(TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(500);
            assertThat(records, is(notNullValue()));
            assertThat(records.isEmpty(), is(false));

            ConsumerRecord<String, String> msg = records.iterator().next();
            assertThat(msg, is(notNullValue()));
            assertThat(msg.key(), is(equalTo(KEY_1)));
            assertThat(msg.value(), is(equalTo(VALUE_1)));
        }
    }
}

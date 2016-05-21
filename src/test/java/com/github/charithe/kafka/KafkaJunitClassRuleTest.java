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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class KafkaJunitClassRuleTest {

    private static final String TOPIC = "topicY";
    private static final String KEY_1 = "keyY1";
    private static final String VALUE_1 = "valueY1";
    private static final String KEY_2 = "keyY2";
    private static final String VALUE_2 = "valueY2";
    private static final String KEY_3 = "keyY3";
    private static final String VALUE_3 = "valueY3";

    @ClassRule
    public static KafkaJunitRule kafkaRule = new KafkaJunitRule();

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
            consumer.commitSync();
        }
    }

    @Test
    public void testMessagesCanBeRead() throws TimeoutException {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY_2, VALUE_2));
        }

        List<String> messages = kafkaRule.readStringMessages(TOPIC, 1, 5);
        assertThat(messages, is(notNullValue()));
        assertThat(messages.size(), is(1));

        String msg = messages.get(0);
        assertThat(msg, is(notNullValue()));
        assertThat(msg, is(equalTo(VALUE_2)));
    }

    @Test(expected = TimeoutException.class)
    public void testTimeout() throws TimeoutException {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY_1, VALUE_2));
        }
        kafkaRule.readStringMessages(TOPIC, 2, 1);
    }

    @Test
    public void testNoDuplicateMessagesAreRead() throws TimeoutException {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY_3, VALUE_3));
        }

        kafkaRule.readStringMessages(TOPIC, 1, 1);
        try {
            kafkaRule.readStringMessages(TOPIC, 1, 1);
            fail("Shouldn't read duplicate messages");
        } catch (TimeoutException e) {
            // expected
        }
    }

    @Test
    public void testExactNumberOfMessagesAreRead() throws TimeoutException {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY_1, VALUE_2));
            producer.send(new ProducerRecord<>(TOPIC, KEY_1, VALUE_3));
            producer.send(new ProducerRecord<>(TOPIC, KEY_2, VALUE_3));
        }

        assertThat(kafkaRule.readStringMessages(TOPIC, 1, 1).size(), is(1));
        assertThat(kafkaRule.readStringMessages(TOPIC, 2, 1).size(), is(2));
    }
}

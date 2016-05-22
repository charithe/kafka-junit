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
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static com.github.charithe.kafka.TestUtil.extractValues;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class KafkaJunitRuleTest {

    private static final String TOPIC = "topicX";
    private static final String KEY_1 = "keyX1";
    private static final String VALUE_1 = "valueX1";
    private static final String KEY_2 = "keyX2";
    private static final String VALUE_2 = "valueX2";
    private static final String KEY_3 = "keyX3";
    private static final String VALUE_3 = "valueX3";

    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule();

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

    @Test
    public void testMessagesCanBeRead() throws Exception {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY_1, VALUE_1));
        }

        List<String> messages = extractValues(kafkaRule.pollStringMessages(TOPIC, 1));
        assertThat(messages, is(notNullValue()));
        assertThat(messages.size(), is(1));

        String msg = messages.get(0);
        assertThat(msg, is(notNullValue()));
        assertThat(msg, is(equalTo(VALUE_1)));
    }

    @Test
    public void testNoDuplicateMessagesAreRead() throws Exception {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY_1, VALUE_1));
        }

        List<String> firstMessageSet = extractValues(kafkaRule.pollStringMessages(TOPIC, 1));
        assertThat(firstMessageSet, hasItem(VALUE_1));

        try {
            List<String> secondMessageSet = extractValues(kafkaRule.pollStringMessages(TOPIC, 1));
            assertThat(secondMessageSet, not(hasItem(VALUE_1)));
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testExactNumberOfMessagesAreRead() throws Exception {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY_1, VALUE_1));
            producer.send(new ProducerRecord<>(TOPIC, KEY_2, VALUE_2));
            producer.send(new ProducerRecord<>(TOPIC, KEY_3, VALUE_3));
        }

        List<String> firstMessageSet = extractValues(kafkaRule.pollStringMessages(TOPIC, 1));
        assertThat(firstMessageSet.size(), is(equalTo(1)));

        List<String> secondMessageSet = extractValues(kafkaRule.pollStringMessages(TOPIC, 2));
        assertThat(secondMessageSet.size(), is(equalTo(2)));
    }
}

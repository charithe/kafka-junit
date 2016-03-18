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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeoutException;

public class KafkaJunitRuleTest {

    private static final String TOPIC = "topicX";
    private static final String KEY = "keyX";
    private static final String VALUE = "valueX";

    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule();

    @Test
    public void testKafkaServerIsUp() {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY, VALUE));
        }

        try (KafkaConsumer<String, String> consumer = kafkaRule.createStringConsumer()) {
            consumer.subscribe(Lists.newArrayList(TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(500);
            assertThat(records, is(notNullValue()));
            assertThat(records.isEmpty(), is(false));

            ConsumerRecord<String, String> msg = records.iterator().next();
            assertThat(msg, is(notNullValue()));
            assertThat(msg.key(), is(equalTo(KEY)));
            assertThat(msg.value(), is(equalTo(VALUE)));
        }
    }

    @Test
    public void testMessagesCanBeRead() throws TimeoutException {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY, VALUE));
        }

        List<String> messages = kafkaRule.readStringMessages(TOPIC, 1, 5);
        assertThat(messages, is(notNullValue()));
        assertThat(messages.size(), is(1));

        String msg = messages.get(0);
        assertThat(msg, is(notNullValue()));
        assertThat(msg, is(equalTo(VALUE)));
    }

    @Test(expected = TimeoutException.class)
    public void testTimeout() throws TimeoutException {
        try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, KEY, VALUE));
        }

        kafkaRule.readStringMessages(TOPIC, 2, 1);
    }
}

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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class KafkaHelperTest {

    private static EphemeralKafkaBroker broker;
    private static KafkaHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        broker = EphemeralKafkaBroker.create();
        broker.start();
        helper = KafkaHelper.createFor(broker);
    }

    @AfterClass
    public static void teardown() {
        broker.stop();
        broker = null;
        helper = null;
    }

    @Test
    public void testProducerAndConsumer() {
        try (KafkaProducer<String, String> producer = helper.createStringProducer()) {
            producer.send(new ProducerRecord<>("test", "k", "v"));
            producer.flush();
        }

        try (KafkaConsumer<String, String> consumer = helper.createStringConsumer()) {
            consumer.subscribe(Lists.newArrayList("test"));
            ConsumerRecords<String, String> records = consumer.poll(500);
            ConsumerRecord<String, String> cr = records.records("test").iterator().next();
            assertThat(cr.key(), is(equalTo("k")));
            assertThat(cr.value(), is(equalTo("v")));
        }
    }

    @Test
    public void testProduceAndConsumeHelpers() throws ExecutionException, InterruptedException {
        helper.produceStrings("my-test-topic", "a", "b", "c", "d", "e");
        List<String> result = helper.consumeStrings("my-test-topic", 5).get();

        assertThat(result, hasSize(5));
        assertThat(result, containsInAnyOrder("a", "b", "c", "d", "e"));
    }

    @Test
    public void testExactNumberOfMessagesAreConsumed() throws Exception {
        helper.produceStrings("topic-a", "valueA", "valueB", "valueC");
        List<String> messages = helper.consumeStrings("topic-a", 2).get();
        assertThat(messages, hasSize(2));
    }

    @Test
    public void testNoDuplicateMessagesAreRead() throws Exception {
        helper.produceStrings("topic-b", "valueA");
        List<String> firstMessageSet = helper.consumeStrings("topic-b", 1).get();
        assertThat(firstMessageSet, hasItem("valueA"));

        helper.produceStrings("topic-b", "valueB", "valueC");
        List<String> secondMessageSet = helper.consumeStrings("topic-b", 1).get();
        assertThat(secondMessageSet, not(hasItem("valueA")));
    }
}

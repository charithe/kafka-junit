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
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
            ConsumerRecords<String, String> records = consumer.poll(10000);
            ConsumerRecord<String, String> cr = records.records("test").iterator().next();
            assertThat(cr.key()).isEqualTo("k");
            assertThat(cr.value()).isEqualTo("v");
        }
    }

    @Test
    public void testProduceAndConsumeHelpers() throws ExecutionException, InterruptedException {
        helper.produceStrings("my-test-topic", "a", "b", "c", "d", "e");
        List<String> result = helper.consumeStrings("my-test-topic", 5).get();

        assertThat(result).hasSize(5);
        assertThat(result).containsExactlyInAnyOrder("a", "b", "c", "d", "e");
    }

    @Test
    public void testExactNumberOfMessagesAreConsumed() throws Exception {
        helper.produceStrings("topic-a", "valueA", "valueB", "valueC");
        List<String> messages = helper.consumeStrings("topic-a", 2).get();
        assertThat(messages).hasSize(2);
    }

    @Test
    public void testNoDuplicateMessagesAreRead() throws Exception {
        helper.produceStrings("topic-b", "valueA");
        List<String> firstMessageSet = helper.consumeStrings("topic-b", 1).get();
        assertThat(firstMessageSet).contains("valueA");

        helper.produceStrings("topic-b", "valueB", "valueC");
        List<String> secondMessageSet = helper.consumeStrings("topic-b", 1).get();
        assertThat(secondMessageSet).doesNotContain("valueA");
    }
}

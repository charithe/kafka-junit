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

import static com.github.charithe.kafka.EphemeralKafkaBrokerTest.TEN_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaJunitClassRuleTest {

    private static final String TOPIC = "topicY";

    @ClassRule
    public static KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());

    @Test
    public void testKafkaServerIsUp() {
        try (KafkaProducer<String, String> producer = kafkaRule.helper().createStringProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, "keyA", "valueA"));
        }

        try (KafkaConsumer<String, String> consumer = kafkaRule.helper().createStringConsumer()) {
            consumer.subscribe(Lists.newArrayList(TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(TEN_SECONDS);
            assertThat(records).isNotNull();
            assertThat(records.isEmpty()).isFalse();

            ConsumerRecord<String, String> msg = records.iterator().next();
            assertThat(msg).isNotNull();
            assertThat(msg.key()).isEqualTo("keyA");
            assertThat(msg.value()).isEqualTo("valueA");
        }
    }
}

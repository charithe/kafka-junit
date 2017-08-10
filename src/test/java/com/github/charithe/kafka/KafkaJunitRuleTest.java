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
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Enclosed.class)
public class KafkaJunitRuleTest {
    private static final String TOPIC = "topicX";

    public static class BaseTest {
        @Rule
        public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());

        @Test
        public void testKafkaServerIsUp() {
            try (KafkaProducer<String, String> producer = kafkaRule.helper().createStringProducer()) {
                producer.send(new ProducerRecord<>(TOPIC, "keyA", "valueA"));
            }

            try (KafkaConsumer<String, String> consumer = kafkaRule.helper().createStringConsumer()) {
                consumer.subscribe(Lists.newArrayList(TOPIC));
                ConsumerRecords<String, String> records = consumer.poll(10000);
                assertThat(records).isNotNull();
                assertThat(records.isEmpty()).isFalse();

                ConsumerRecord<String, String> msg = records.iterator().next();
                assertThat(msg).isNotNull();
                assertThat(msg.key()).isEqualTo("keyA");
                assertThat(msg.value()).isEqualTo("valueA");
            }
        }
    }

    public static class WaitForStartup {
        @Rule
        public KafkaJunitRule kafkaRule = KafkaJunitRule.create()
            .waitForStartup();

        @Test
        public void testKafkaServerIsUp() {
            // Setup Zookeeper client
            final String zkConnectionString = kafkaRule.helper().zookeeperConnectionString();
            final ZkClient zkClient = new ZkClient(
                zkConnectionString, 1000, 8000, ZKStringSerializer$.MODULE$
            );
            final ZkConnection zkConnection = new ZkConnection(zkConnectionString);
            final ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);

            // Create topic
            AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), null);

            // Produce/consume test
            try (KafkaProducer<String, String> producer = kafkaRule.helper().createStringProducer()) {
                producer.send(new ProducerRecord<>(TOPIC, "keyA", "valueA"));
            }

            try (KafkaConsumer<String, String> consumer = kafkaRule.helper().createStringConsumer()) {
                consumer.subscribe(Lists.newArrayList(TOPIC));
                ConsumerRecords<String, String> records = consumer.poll(10000);
                assertThat(records).isNotNull();
                assertThat(records.isEmpty()).isFalse();

                ConsumerRecord<String, String> msg = records.iterator().next();
                assertThat(msg).isNotNull();
                assertThat(msg.key()).isEqualTo("keyA");
                assertThat(msg.value()).isEqualTo("valueA");
            }
        }
    }
}

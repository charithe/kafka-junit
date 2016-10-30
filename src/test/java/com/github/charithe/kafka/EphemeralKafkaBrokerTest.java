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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class EphemeralKafkaBrokerTest {

    @Test
    public void testStartAndStop() throws Exception {
        int kafkaPort = InstanceSpec.getRandomPort();
        int zkPort = InstanceSpec.getRandomPort();
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(kafkaPort, zkPort);
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning(), is(true));
        assertThat(broker.getKafkaPort().get(), is(equalTo(kafkaPort)));
        assertThat(broker.getZookeeperPort().get(), is(equalTo(zkPort)));
        assertThat(broker.getBrokerList().isPresent(), is(true));
        assertThat(broker.getZookeeperConnectString().isPresent(), is(true));
        assertThat(broker.getLogDir().isPresent(), is(true));

        Path logDir = Paths.get(broker.getLogDir().get());
        assertThat(Files.exists(logDir), is(true));

        broker.stop();
        assertThat(res.isDone(), is(true));
        assertThat(broker.isRunning(), is(false));
        assertThat(broker.getBrokerList().isPresent(), is(false));
        assertThat(broker.getZookeeperConnectString().isPresent(), is(false));
        assertThat(Files.exists(logDir), is(false));
    }

    @Test
    public void testReadAndWrite() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create();
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning(), is(true));

        try (KafkaProducer<String, String> producer =
                     broker.createProducer(new StringSerializer(), new StringSerializer(), null)) {
            Future<RecordMetadata> result =
                    producer.send(new ProducerRecord<>("test-topic", "key1", "value1"));

            RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
            assertThat(metadata, is(notNullValue()));
            assertThat(metadata.topic(), is(equalTo("test-topic")));
        }

        try (KafkaConsumer<String, String> consumer =
                     broker.createConsumer(new StringDeserializer(), new StringDeserializer(), null)) {

            consumer.subscribe(Lists.newArrayList("test-topic"));
            ConsumerRecords<String, String> records = consumer.poll(500);
            assertThat(records, is(notNullValue()));
            assertThat(records.isEmpty(), is(false));

            ConsumerRecord<String, String> msg = records.iterator().next();
            assertThat(msg, is(notNullValue()));
            assertThat(msg.key(), is(equalTo("key1")));
            assertThat(msg.value(), is(equalTo("value1")));
        }

        broker.stop();
    }
}

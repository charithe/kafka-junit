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
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KafkaHelper {

    private final EphemeralKafkaBroker broker;

    public static KafkaHelper createFor(EphemeralKafkaBroker broker) {
        return new KafkaHelper(broker);
    }

    KafkaHelper(EphemeralKafkaBroker broker) {this.broker = broker;}

    /**
     * Get the producer configuration
     *
     * @return Properties
     */
    public Properties producerConfig() {
        return broker.producerConfig();
    }

    /**
     * Get the consumer configuration (with auto-commit enabled)
     *
     * @return Properties
     */
    public Properties consumerConfig() {
        return broker.consumerConfig();
    }

    /**
     * Get the consumer configuration
     *
     * @param enableAutoCommit Enable auto-commit
     * @return Properties
     */
    public Properties consumerConfig(boolean enableAutoCommit) {
        return broker.consumerConfig(enableAutoCommit);
    }

    /**
     * Get the zookeeper connection string
     *
     * @return zookeeper connection string or {@link IllegalStateException} if the broker is not running
     */
    public String zookeeperConnectionString() {
        return broker.getZookeeperConnectString().orElseThrow(() -> new IllegalStateException("KafkaBroker is not " +
                "running"));
    }

    /**
     * Get the zookeeper port
     *
     * @return zookeeper port of or {@link IllegalStateException} if the broker is not running
     */
    public int zookeeperPort() {
        return broker.getZookeeperPort().orElseThrow(() -> new IllegalStateException("KafkaBroker is not running"));
    }

    /**
     * Get the broker listener port
     *
     * @return broker listener port or {@link IllegalStateException} if the broker is not running
     */
    public int kafkaPort() {
        return broker.getKafkaPort().orElseThrow(() -> new IllegalStateException("KafkaBroker is not running"));
    }

    /**
     * Create a producer that can write to this broker
     *
     * @param keySerializer   Key serializer class
     * @param valueSerializer Valuer serializer class
     * @param overrideConfig  Producer config to override. Pass null if there aren't any.
     * @param <K>             Type of Key
     * @param <V>             Type of Value
     * @return KafkaProducer
     */
    public <K, V> KafkaProducer<K, V> createProducer(Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                                     Properties overrideConfig) {
        return broker.createProducer(keySerializer, valueSerializer, overrideConfig);
    }

    /**
     * Create a producer that writes String keys and values
     *
     * @return KafkaProducer
     */
    public KafkaProducer<String, String> createStringProducer() {
        return createProducer(new StringSerializer(), new StringSerializer(), null);
    }

    /**
     * Create a producer that writes String keys and values
     *
     * @param overrideConfig Producer config to override
     * @return KafkaProducer
     */
    public KafkaProducer<String, String> createStringProducer(Properties overrideConfig) {
        return createProducer(new StringSerializer(), new StringSerializer(), overrideConfig);
    }

    /**
     * Create a producer that writes byte keys and values
     *
     * @return KafkaProducer
     */
    public KafkaProducer<byte[], byte[]> createByteProducer() {
        return createProducer(new ByteArraySerializer(), new ByteArraySerializer(), null);
    }

    /**
     * Create a producer that writes byte keys and values
     *
     * @param overrideConfig Producer config to override
     * @return KafkaProducer
     */
    public KafkaProducer<byte[], byte[]> createByteProducer(Properties overrideConfig) {
        return createProducer(new ByteArraySerializer(), new ByteArraySerializer(), overrideConfig);
    }

    /**
     * Produce data to the specified topic
     *
     * @param topic    Topic to produce to
     * @param producer Producer to use
     * @param data     Data to produce
     * @param <K>      Type of key
     * @param <V>      Type of value
     */
    public <K, V> void produce(String topic, KafkaProducer<K, V> producer, Map<K, V> data) {
        data.forEach((k, v) -> producer.send(new ProducerRecord<>(topic, k, v)));
        producer.flush();
    }

    /**
     * Convenience method to produce a set of strings to the specified topic
     *
     * @param topic  Topic to produce to
     * @param values Values produce
     */
    public void produceStrings(String topic, String... values) {
        try (KafkaProducer<String, String> producer = createStringProducer()) {
            Map<String, String> data = Arrays.stream(values)
                    .collect(Collectors.toMap(k -> String.valueOf(k.hashCode()), Function.identity()));
            produce(topic, producer, data);
        }
    }

    /**
     * Create a consumer that can read from this broker
     *
     * @param keyDeserializer   Key deserializer
     * @param valueDeserializer Value deserializer
     * @param overrideConfig    Consumer config to override. Pass null if there aren't any
     * @param <K>               Type of Key
     * @param <V>               Type of Value
     * @return KafkaConsumer
     */
    public <K, V> KafkaConsumer<K, V> createConsumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
                                                     Properties overrideConfig) {
        return broker.createConsumer(keyDeserializer, valueDeserializer, overrideConfig);
    }

    /**
     * Create a consumer that reads strings
     *
     * @return KafkaConsumer
     */
    public KafkaConsumer<String, String> createStringConsumer() {
        return createConsumer(new StringDeserializer(), new StringDeserializer(), null);
    }

    /**
     * Create a consumer that reads strings
     *
     * @param overrideConfig Consumer config to override
     * @return KafkaConsumer
     */
    public KafkaConsumer<String, String> createStringConsumer(Properties overrideConfig) {
        return createConsumer(new StringDeserializer(), new StringDeserializer(), overrideConfig);
    }

    /**
     * Create a consumer that reads bytes
     *
     * @return KafkaConsumer
     */
    public KafkaConsumer<byte[], byte[]> createByteConsumer() {
        return createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), null);
    }

    /**
     * Create a consumer that reads bytes
     *
     * @param overrideConfig Consumer config to override
     * @return KafkaConsumer
     */
    public KafkaConsumer<byte[], byte[]> createByteConsumer(Properties overrideConfig) {
        return createConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), overrideConfig);
    }

    /**
     * Attempt to consume the specified number of messages
     *
     * @param topic                Topic to consume
     * @param consumer             Consumer to use
     * @param numMessagesToConsume Number of messages to consume
     * @param <K>                  Type of Key
     * @param <V>                  Type of Value
     * @return ListenableFuture
     */
    public <K, V> ListenableFuture<List<ConsumerRecord<K, V>>> consume(String topic, KafkaConsumer<K, V> consumer, int numMessagesToConsume) {
        consumer.subscribe(Lists.newArrayList(topic));
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        return executor.submit(new RecordConsumer<>(numMessagesToConsume, consumer));
    }

    /**
     * Consume specified number of string messages
     *
     * @param topic                Topic to consume from
     * @param numMessagesToConsume Number of messages to consume
     * @return ListenableFuture
     */
    public ListenableFuture<List<String>> consumeStrings(String topic, int numMessagesToConsume) {
        KafkaConsumer<String, String> consumer = createStringConsumer();
        ListenableFuture<List<ConsumerRecord<String, String>>> records = consume(topic, consumer, numMessagesToConsume);
        return Futures.transform(records, this::extractValues, MoreExecutors.directExecutor());
    }

    private List<String> extractValues(List<ConsumerRecord<String, String>> records) {
        if (records == null) {
            return Collections.emptyList();
        }
        return records.stream().map(ConsumerRecord::value).collect(Collectors.toList());
    }

    public static class RecordConsumer<K, V> implements Callable<List<ConsumerRecord<K, V>>> {
        private final int numRecordsToPoll;
        private final KafkaConsumer<K, V> consumer;

        RecordConsumer(int numRecordsToPoll, KafkaConsumer<K, V> consumer) {
            this.numRecordsToPoll = numRecordsToPoll;
            this.consumer = consumer;
        }

        @Override
        public List<ConsumerRecord<K, V>> call() throws Exception {
            try {
                Map<TopicPartition, OffsetAndMetadata> commitBuffer = Maps.newHashMap();
                List<ConsumerRecord<K, V>> polledMessages = new ArrayList<>(numRecordsToPoll);
                while ((polledMessages.size() < numRecordsToPoll) && (!Thread.currentThread().isInterrupted())) {
                    ConsumerRecords<K, V> records = consumer.poll(0);
                    for (ConsumerRecord<K, V> rec : records) {
                        polledMessages.add(rec);
                        commitBuffer.put(
                                new TopicPartition(rec.topic(), rec.partition()),
                                new OffsetAndMetadata(rec.offset() + 1)
                        );

                        if (polledMessages.size() == numRecordsToPoll) {
                            consumer.commitSync(commitBuffer);
                            break;
                        }
                    }
                }
                return polledMessages;
            } finally {
                consumer.close();
            }
        }
    }
}

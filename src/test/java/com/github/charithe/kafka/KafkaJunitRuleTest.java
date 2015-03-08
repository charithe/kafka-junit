/*
 * Copyright 2015 Charith Ellawala
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

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class KafkaJunitRuleTest {

    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule();

    @Test
    public void testKafkaServerIsUp() {
        ProducerConfig conf = kafkaRule.producerConfig();
        Producer<String, String> producer = new Producer<>(conf);
        producer.send(new KeyedMessage<>("topic", "k1", "value1"));
        producer.close();

        ConsumerConfig consumerConf = kafkaRule.consumerConfig();
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConf);

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("topic", 1);
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder(consumerConf.props()), new StringDecoder(consumerConf.props()));
        List<KafkaStream<String, String>> streams = consumerMap.get("topic");


        assertThat(streams, is(notNullValue()));
        assertThat(streams.size(), is(equalTo(1)));

        KafkaStream<String, String> ks = streams.get(0);
        ConsumerIterator<String, String> iterator = ks.iterator();
        MessageAndMetadata<String, String> msg = iterator.next();
        assertThat(msg, is(notNullValue()));
        assertThat(msg.key(), is(equalTo("k1")));
        assertThat(msg.message(), is(equalTo("value1")));
    }

}

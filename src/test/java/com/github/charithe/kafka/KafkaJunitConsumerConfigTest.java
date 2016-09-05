package com.github.charithe.kafka;


import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class KafkaJunitConsumerConfigTest {

    private static final int KAFKA_PORT = 1234;
    private static final int ZOOKEEPER_PORT = 5678;
    private static final Properties BROKER_PROP = new Properties();
    private static final Properties CONSUMER_PROP = new Properties();
    private static final String KEY_1 = "test.key.1";
    private static final String VALUE_1 = "test.value.1";
    private static final String GROUP_KEY = "group.id";
    private static final String GROUP_VALUE = "group.key";

    @Test
    public void testDefaultConsumerConfig() {

        KafkaJunitRule kafkaRule = new KafkaJunitRule(KAFKA_PORT, ZOOKEEPER_PORT, BROKER_PROP, CONSUMER_PROP);

        assertThat(kafkaRule.consumerConfig().getProperty("enable.auto.commit"), equalTo("true"));
        assertThat(kafkaRule.consumerConfig().getProperty(GROUP_KEY), equalTo("kafka-junit-consumer"));
        assertThat(kafkaRule.consumerConfig(false).getProperty("enable.auto.commit"), equalTo("false"));
    }

    @Test
    public void testConsumerConfig() {

        CONSUMER_PROP.put(KEY_1, VALUE_1);
        CONSUMER_PROP.put(GROUP_KEY, GROUP_VALUE);

        KafkaJunitRule kafkaRule = new KafkaJunitRule(KAFKA_PORT, ZOOKEEPER_PORT, BROKER_PROP, CONSUMER_PROP);

        assertThat(kafkaRule.consumerConfig().getProperty(KEY_1), equalTo(VALUE_1));
        assertThat(kafkaRule.consumerConfig().getProperty(GROUP_KEY), equalTo(GROUP_VALUE));
    }
}

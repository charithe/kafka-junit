Kafka JUnit Rule [![Build Status](https://travis-ci.org/charithe/kafka-junit.svg?branch=master)](https://travis-ci.org/charithe/kafka-junit) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit)
=================

JUnit rule for starting and tearing down a Kafka broker during tests.

Version | Kafka Version 
--------|---------------
1.6     | 0.8.2.1       
1.7     | 0.8.2.2       
1.8     | 0.9.0.0       


Installation
-------------

Releases are available on Maven Central.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit)


Snapshot versions containing builds from the latest `master` are available in the Sonatype snapshots repo.

Usage
------

Create an instance of the rule in your test class and annotate it with `@Rule`. This will start and stop the
broker between each test invocation.

 ```java
 @Rule
 public KafkaJunitRule kafkaRule = new KafkaJunitRule();
 ```


 To spin up the broker at the beginning of a test suite and tear it down at the end, use `@ClassRule`.

 ```java
 @ClassRule
 public static KafkaJunitRule kafkaRule = new KafkaJunitRule();
 ```



`kafkaRule` can be referenced from within your test methods to obtain information about the Kafka broker.

```java
@Test
public void testSomething(){
    // Use the built-in sync producer configuration
    ProducerConfig producerConfig = kafkaRule.producerConfigWithStringEncoder();

    // Use the built-in consumer configuration
    ConsumerConfig consumerConfig = kafkaRule.consumerConfig();

    // Alternatively, the Zookeeper connection String and the broker port can be retrieved to generate your own config
    String zkConnStr = kafkaRule.zookeeperConnectionString();
    int brokerPort = kafkaRule.kafkaBrokerPort();

    ...
}
```



`kafkaRule` can also be used from within your test methods to read messages from a Kafka topic. The rule provides an utility method that simplifies assertion code in tests:

```java
@Test
public void testStringMessageIsDelivered() throws TimeoutException {
    // Create a Kafka producer using the built-in producer configuration
    ProducerConfig conf = kafkaRule.producerConfigWithStringEncoder();
    Producer<String, String> producer = new Producer<>(conf);
    producer.send(new KeyedMessage<>("topic", "key", "value"));
    producer.close();

    List<String> messages = kafkaRule.readStringMessages("topic", 1);
    assertThat(messages, is(notNullValue()));
    assertThat(messages.size(), is(1));
    assertThat(messages.get(0), is("value"));
    ...
}
```

`kafkaRule.readStringMessages(topic, numberOfMessages)` uses `kafka.serializer.StringDecoder` to convert messages to `String` objects. Alternatively, `kafkaRule.readMessages(topic, numberOfMessages, decoder)` can be used with a custom message decoder:

```java
@Test
public void testCustomMessageIsDelivered() throws TimeoutException {
    // IdNameBean is a POJO with 2 properties: id and name
    final IdNameBean idNameBean = new IdNameBean("someId","someName");
    
    // Create a JSON string out of a bean, in this case {"id": "someId","name": "someName"}
    final String IdNameBeanJson = toJson(idNameBean);

    // Create a Kafka producer using the built-in producer configuration
    ProducerConfig conf = kafkaRule.producerConfigWithStringEncoder();
    Producer<String, String> producer = new Producer<>(conf);
    producer.send(new KeyedMessage<>("topic", "key", IdNameBeanJson));
    producer.close();

    List<IdNameBean> messages = kafkaRule.readMessages("topic", 1, new IdNameBeanJsonDecoder());
    assertThat(messages, is(notNullValue()));
    assertThat(messages.size(), is(1));
    assertThat(messages.get(0), is(idNameBean));
    ...
}
```

`kafkaRule.readMessages()` and `kafkaRule.readStringMessages()` will block for 5 seconds until all expected messages are read. A `java.util.concurrent.TimeoutException` will be thrown if not all the expected messages can be retrieved from the topic:

```java
@Test(expected=TimeoutException.class)
public void testTimeout() throws TimeoutException {
    // Create a Kafka producer using the built-in producer configuration
    ProducerConfig conf = kafkaRule.producerConfigWithStringEncoder();
    Producer<String, String> producer = new Producer<>(conf);
    producer.send(new KeyedMessage<>("topic", "key", "value"));
    producer.close();

    // Expect 2 messages but only 1 has been sent
    kafkaRule.readStringMessages("topic", 2);
}
```

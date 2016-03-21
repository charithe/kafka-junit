Kafka JUnit Rule [![Build Status](https://travis-ci.org/charithe/kafka-junit.svg?branch=master)](https://travis-ci.org/charithe/kafka-junit) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit)
=================

JUnit rule for starting and tearing down a Kafka broker during tests.

Version | Kafka Version 
--------|---------------
1.6     | 0.8.2.1       
1.7     | 0.8.2.2       
1.8     | 0.9.0.0  
2.1     | 0.9.0.1


Please note that version 2.x contains some breaking API changes.

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
    // Use the built-in producer
    KafkaProducer<String, String> producer = kafkaRule.createStringProducer();

    // Use the built-in consumer 
    KafkaConsumer<String, String> consumer = kafkaRule.createStringConsumer();

    // Alternatively, the Zookeeper connection String and the broker port can be retrieved to generate your own config
    String zkConnStr = kafkaRule.zookeeperConnectionString();
    int brokerPort = kafkaRule.kafkaBrokerPort();

    ...
}
```



There are also helper methods available to read a number of messages with a configurable timeout. 

```java
@Test
public void testMessagesCanBeRead() throws TimeoutException {
    // write a message 
    try (KafkaProducer<String, String> producer = kafkaRule.createStringProducer()) {
        producer.send(new ProducerRecord<>(TOPIC, KEY, VALUE));
    }

    // attempt to read a single message with a 5 second timeout
    List<String> messages = kafkaRule.readStringMessages(TOPIC, 1, 5);
    assertThat(messages, is(notNullValue()));
    assertThat(messages.size(), is(1));

    String msg = messages.get(0);
    assertThat(msg, is(notNullValue()));
    assertThat(msg, is(equalTo(VALUE)));
}
```

Refer to unit tests for more examples.

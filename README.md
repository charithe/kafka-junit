Kafka JUnit Rule [![Build Status](https://travis-ci.org/charithe/kafka-junit.svg?branch=master)](https://travis-ci.org/charithe/kafka-junit) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit)
=================

JUnit rule for starting and tearing down a Kafka broker during tests.

**Please note that version 3.x.x drops Java 7 support and contains breaking API changes.** 


Version | Kafka Version 
--------|---------------
1.6     | 0.8.2.1       
1.7     | 0.8.2.2       
1.8     | 0.9.0.0  
2.3     | 0.9.0.1
2.4     | 0.10.0.0
2.5     | 0.10.0.1
3.0.0   | 0.10.0.1
3.0.1   | 0.10.0.1
3.0.2   | 0.10.1.1
3.0.3   | 0.10.2.0
3.0.4   | 0.10.2.1
3.1.0   | 0.11.0.0
3.1.1   | 0.11.0.1

Installation
-------------

Releases are available on Maven Central.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.charithe/kafka-junit)


Snapshot versions containing builds from the latest `master` are available in the Sonatype snapshots repo.

Javadocs
--------

<http://charithe.github.io/kafka-junit/>

Usage
------

Create an instance of the rule in your test class and annotate it with `@Rule`. This will start and stop the
broker between each test invocation.

 ```java
 @Rule
 public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());
 ```


 To spin up the broker at the beginning of a test suite and tear it down at the end, use `@ClassRule`.

 ```java
 @ClassRule
 public static KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());
 ```



`kafkaRule` can be referenced from within your test methods to obtain information about the Kafka broker.

```java
@Test
public void testSomething(){
    // Convenience methods to produce and consume messages
    kafkaRule.helper().produceStrings("my-test-topic", "a", "b", "c", "d", "e");
    List<String> result = kafkaRule.helper().consumeStrings("my-test-topic", 5).get();

    // or use the built-in producers and consumers
    KafkaProducer<String, String> producer = kafkaRule.helper().createStringProducer();

    KafkaConsumer<String, String> consumer = kafkaRule.helper().createStringConsumer();

    // Alternatively, the Zookeeper connection String and the broker port can be retrieved to generate your own config
    String zkConnStr = kafkaRule.helper().zookeeperConnectionString();
    int brokerPort = kafkaRule.helper().kafkaPort();
}
```

`EphemeralKafkaBroker` contains the core logic used by the JUnit rule and can be used independently. 

`KafkaHelper` contains a bunch of convenience methods to work with the `EphemeralKafkaBroker` 

Refer to [Javadocs](http://charithe.github.io/kafka-junit/) and unit tests for more usage examples.

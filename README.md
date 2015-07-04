Kafka JUnit Rule
=================

A work-in-progress JUnit rule for starting and tearing down a Kafka broker during tests.


Installation
-------------

Release are available on Maven Central.


```xml
<dependency>
    <groupId>com.github.charithe</groupId>
    <artifactId>kafka-junit</artifactId>
    <version>1.3</version>
</dependency>
```


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
    ProducerConfig producerConfig = kafkaRule.producerConfig();

    // Use the built-in consumer configuration
    ConsumerConfig consumerConfig = kafkaRule.consumerConfig();

    // Alternatively, the Zookeeper connection String and the broker port can be retrieved to generate your own config
    String zkConnStr = kafkaRule.zookeeperConnectionString();
    int brokerPort = kafkaRule.kafkaBrokerPort();

    ...
}
```

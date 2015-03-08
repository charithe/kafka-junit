Kafka JUnit Rule
=================

A work-in-progress JUnit rule for starting and tearing down a Kafka broker during tests.


Installation
-------------

Use https://jitpack.io/ until I get around to doing a proper release to Maven Central.


Usage
------

Create an instance of the rule in your test class and annotate it with `@Rule`.

 ```
 @Rule
 public KafkaJunitRule kafkaRule = new KafkaJunitRule();
 ```

`kafkaRule` can now be referenced from within your test methods.

```
@Test
public void testSomething(){
    // Use the built-in sync producer configuration
    ProducerConfig producerConfig = kafkaRule.producerConfig();

    // Use the bult-in consumer configuration
    ConsumerConfig consumerConfig = kafkaRule.consumerConfig();

    // Alternatively, the Zookeeper connection String and the broker port can be retrieved to generate your own config
    String zkConnStr = kafkaRule.zookeeperConnectionString();
    int brokerPort = kafkaRule.kafkaBrokerPort();

    ...
}
```
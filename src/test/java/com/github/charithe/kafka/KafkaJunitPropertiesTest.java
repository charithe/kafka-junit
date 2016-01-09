package com.github.charithe.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Rule;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by eh on 1/8/16.
 */
public class KafkaJunitPropertiesTest {

    public static Properties kafkaProps=new Properties();
    static{
        kafkaProps.setProperty("num.partitions","5");
    }
    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule(9095,2014,kafkaProps);

    @Test(timeout = 10000)
    public void ensureMultiplePartitionsTest() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9095");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(props);
        ProducerRecord<String,String> record;

        record=new ProducerRecord<String, String>("newTopic",0,"key","value");
        Future<RecordMetadata> f =producer.send(record);
        assertEquals(f.get().partition(),0);

        record=new ProducerRecord<String, String>("newTopic",3,"key","value");
        f =producer.send(record);
        assertEquals(f.get().partition(),3);

        record=new ProducerRecord<String, String>("newTopic",4,"key","value");
        f =producer.send(record);
        assertEquals(f.get().partition(),4);
    }
}

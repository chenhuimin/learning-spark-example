package com.laanto.it.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by user on 2016/3/11.
 */
public class CustomPartitionConsumer {
    private static KafkaConsumer<String, String> consumer;

    public CustomPartitionConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1.laanto.com:9092,node2.laanto.com:9092,node3.laanto.com:9092");
        props.put("group.id", "group1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "5000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
    }

    public static void main(String[] args) {
        int argsCount = args.length;
        if (argsCount == 0) {
            throw new IllegalArgumentException("Please provide topic name as arguments");
        }
        String topic = args[0];
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1.laanto.com:9092,node2.laanto.com:9092,node3.laanto.com:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Topic Name - " + topic);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }

//        CustomPartitionConsumer consumer = new CustomPartitionConsumer();
//        System.out.println("Topic Name - " + topic);
//        consumer.subscribeMessage(topic);
        // System.out.println(String.format("key=%s,offset=%d,partition=%d,topic1=%s,value=%s", "test", 10l, 1, "kafkaTopic", "message1"));

    }

    private void subscribeMessage(String topic) {
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                long offset = record.offset();
                int partition = record.partition();
                String topic1 = record.topic();
                String value = record.value();
                System.out.println(String.format("key=%s,offset=%d,partition=%d,topic1=%s,value=%s", key, offset, partition, topic1, value));
            }
        }
    }
}

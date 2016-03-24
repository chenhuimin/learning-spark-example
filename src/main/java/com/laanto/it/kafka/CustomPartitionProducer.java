package com.laanto.it.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by user on 2016/3/9.
 */
public class CustomPartitionProducer {
    // private static Producer<String, String> producer;
    private static KafkaProducer<String, String> producer;

    public CustomPartitionProducer() {
        Properties props = new Properties();
//        props.put("metadata.broker.list", "node1.laanto.com:9092,node2.laanto.com:9092,node3.laanto.com:9092");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "com.laanto.it.kafka.SimplePartitioner");
//        props.put("request.required.acks", "1");
        //  ProducerConfig config = new ProducerConfig(props);
        // producer = new Producer<String, String>(config);
        props.put("bootstrap.servers", "node1.laanto.com:9092,node2.laanto.com:9092,node3.laanto.com:9092");
        props.put("acks", "1");
        props.put("retries", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.laanto.it.kafka.SimplePartitioner");
        producer = new KafkaProducer<String, String>(props);
    }

    public static void main(String[] args) {
        int argsCount = args.length;
        if (argsCount == 0 || argsCount == 1) {
            throw new IllegalArgumentException("Please provide topic name and Message count as arguments");
        }
        String topic = args[0];
        String count = args[1];
        int messageCount = Integer.parseInt(count);
        System.out.println("Topic Name - " + topic);
        System.out.println("Message Count - " + messageCount);
        CustomPartitionProducer customPartitionProducer = new CustomPartitionProducer();
        customPartitionProducer.publishMessage(topic, messageCount);

    }

    private void publishMessage(String topic, int messageCount) {
        Random random = new Random();
        for (int mCount = 0; mCount < messageCount; mCount++) {
            String clientIP = "192.168.14." + random.nextInt(255);
            String accessTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            String message = accessTime + ",kafka.apache.org," + clientIP;
            System.out.println(message);
            // KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, clientIP, message);
            final ProducerRecord<String, String> data = new ProducerRecord<>(topic, clientIP, message);
            producer.send(data);

        }
        producer.close();
    }
}


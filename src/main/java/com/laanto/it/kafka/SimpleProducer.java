package com.laanto.it.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;

/**
 * Created by user on 2016/3/9.
 */
public class SimpleProducer {
    private static Producer<String, String> producer;

    public SimpleProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "node1.laanto.com:9092,node2.laanto.com:9092,node3.laanto.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
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
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.publishMessage(topic, messageCount);

    }

    private void publishMessage(String topic, int messageCount) {
        for (int mCount = 0; mCount < messageCount; mCount++) {
            String runtime = new Date().toString();
            String msg = "Message Publishing Time - " + runtime;
            System.out.println(msg);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
            producer.send(data);
        }
        producer.close();
    }

}

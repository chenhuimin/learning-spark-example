package com.laanto.it.kafka;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by user on 2016/3/9.
 */
public class SimplePartitioner implements Partitioner {

//    @Override
//    public int partition(Object key, int numPartitions) {
//        int partition = 0;
//        String partitionKey = (String) key;
//        int offset = partitionKey.lastIndexOf('.');
//        if (offset > 0) {
//            partition = Integer.parseInt(partitionKey.substring(offset + 1)) % numPartitions;
//        }
//        return partition;
//    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        System.out.println("numPartitions="+numPartitions);
        int partition = 0;
        if (numPartitions > 0) {
            String partitionKey = (String) key;
            int offset = partitionKey.lastIndexOf('.');
            if (offset > 0) {
                partition = Integer.parseInt(partitionKey.substring(offset + 1)) % numPartitions;
            }
        }
        System.out.println("partition="+partition);
        return partition;

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

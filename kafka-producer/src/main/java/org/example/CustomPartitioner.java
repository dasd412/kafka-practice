package org.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //리턴 값은 주어진 레코드가 들어갈 파티션 번호

        if (keyBytes == null) {
            throw new InvalidRecordException("Need Message key");
        }
        if (((String) key).equals("year")) {
            return 0;
        }

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numberOfPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes) % numberOfPartitions);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

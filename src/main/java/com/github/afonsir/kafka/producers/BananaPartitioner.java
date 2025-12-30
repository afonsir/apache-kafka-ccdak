package com.github.afonsir.kafka.producers;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;

import io.confluent.common.utils.Utils;

public class BananaPartitioner implements Partitioner {

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(String topic, Object key, byte[] keyBytes,
                       Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();

    if ((keyBytes == null) || (!(key instanceof String)))
      throw new InvalidRecordException("We expect all messages to have customer name as key");

    if (((String) key).equals("Banana"))
      return numPartitions - 1;

    return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
  }

  @Override
  public void close() {}
}

package com.github.afonsir.kafka.consumers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StandaloneConsumer {
  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:19092,localhost:29092"
    );

    props.put(ConsumerConfig.GROUP_ID_CONFIG, "StandaloneCountryCounter");

    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class.getName()
    );

    props.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class.getName()
    );

    try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
      Duration timeout = Duration.ofMillis(100);

      List<TopicPartition> partitions = new ArrayList<>();
      List<PartitionInfo> partitionInfos = null;

      partitionInfos = consumer.partitionsFor("customer.countries");

      if (partitionInfos != null) {
        for (PartitionInfo partition : partitionInfos)
          partitions.add(new TopicPartition(partition.topic(), partition.partition()));

        consumer.assign(partitions);

        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(timeout);

          for (ConsumerRecord<String, String> record : records) {
            System.out.printf(
              "topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value()
            );
          }

          consumer.commitSync();
        }
      }
    }
  }
}

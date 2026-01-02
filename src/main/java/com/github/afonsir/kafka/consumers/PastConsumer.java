package com.github.afonsir.kafka.consumers;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PastConsumer {
  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:19092,localhost:29092"
    );

    props.put(ConsumerConfig.GROUP_ID_CONFIG, "PastCountryCounter");

    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class.getName()
    );

    props.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class.getName()
    );

    try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList("customer.countries"));

      Duration timeout = Duration.ofMillis(100);
      long oneHourEarlier = Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli();

      while (consumer.assignment().isEmpty()) {
        consumer.poll(timeout);
      }

      System.out.println("Assigned partitions: " + consumer.assignment());

      Map<TopicPartition, Long> partitionTimestampMap =
        consumer.assignment()
                .stream()
                .collect(Collectors.toMap(tp -> tp, tp -> oneHourEarlier));

      Map<TopicPartition, OffsetAndTimestamp> offsetMap =
        consumer.offsetsForTimes(partitionTimestampMap);

      for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetMap.entrySet()) {
        OffsetAndTimestamp oat = entry.getValue();

        if (oat != null)
          consumer.seek(entry.getKey(), oat.offset());
      }

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);

        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
            "topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
            record.topic(), record.partition(), record.offset(), record.key(), record.value()
          );
        }
      }
    }
  }
}

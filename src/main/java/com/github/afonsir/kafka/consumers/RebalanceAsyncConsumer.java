package com.github.afonsir.kafka.consumers;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceAsyncConsumer {
  private static final Logger log = LoggerFactory.getLogger(AsyncConsumer.class);
  private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  private static Consumer<String, String> consumer;

  private static class RebalanceHandler implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      System.out.println(
        "Lost partitions in rebalance. " +
        "Commiting current offsets: " + currentOffsets
      );
      
      consumer.commitSync(currentOffsets);
    }
  };

  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:19092,localhost:39092"
    );

    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "RebalanceAsyncCountryCounter");

    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class.getName()
    );

    props.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class.getName()
    );

    try {
      consumer = new KafkaConsumer<>(props);

      consumer.subscribe(
        Collections.singletonList("customer.countries"),
        new RebalanceHandler()
      );

      int recordCount = 0;
      Duration timeout = Duration.ofMillis(100);

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);

        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
            "topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
            record.topic(), record.partition(), record.offset(), record.key(), record.value()
          );

          currentOffsets.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1, "none")
          );

          if (recordCount % 1000 == 0)
            consumer.commitAsync(currentOffsets, null);

          recordCount++;
        }
      }
    } catch (WakeupException e) {
      // ignore, we're closing

    } catch (Exception e) {
      log.error("Unexpected error", e);

    } finally {
      try {
        consumer.commitSync(currentOffsets);

      } finally {
        consumer.close();
        System.out.println("Closed consumer and we are done");
      }
    }
  }
}

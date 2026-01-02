package com.github.afonsir.kafka.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncAndSyncConsumer {
  private static volatile boolean closing = false;
  private static final Logger log = LoggerFactory.getLogger(AsyncAndSyncConsumer.class);

  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:29092,localhost:39092"
    );

    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "AsyncAndSyncCountryCounter");

    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class.getName()
    );

    props.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class.getName()
    );

    Consumer<String, String> consumer = new KafkaConsumer<>(props);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println(">>> Shutdown hook executed");

      closing = true;
      consumer.wakeup();
    }));

    try {
      consumer.subscribe(Collections.singletonList("customer.countries"));

      Duration timeout = Duration.ofMillis(100);

      while (!closing) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);

        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
            "topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
            record.topic(), record.partition(), record.offset(), record.key(), record.value()
          );
        }

        if (!records.isEmpty()) {
          System.out.println("Commiting async");
          consumer.commitAsync();
        }
      }
    } catch (WakeupException e) {
      if (!closing) throw e;

    } catch (Exception e) {
      log.error("Unexpected error", e);

    } finally {
      System.out.println("Commiting sync");
      consumer.commitSync();
      consumer.close();
    }
  }
}

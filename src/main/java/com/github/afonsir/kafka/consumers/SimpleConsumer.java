package com.github.afonsir.kafka.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

public class SimpleConsumer {
  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:19092,localhost:29092"
    );

    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");

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
      Map<String, Integer> customerCountryMap = new HashMap<>();

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);

        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
            "topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
            record.topic(), record.partition(), record.offset(), record.key(), record.value()
          );

          int updatedCount = 1;

          if (customerCountryMap.containsKey(record.value()))
            updatedCount = customerCountryMap.get(record.value()) + 1;

          customerCountryMap.put(record.value(), updatedCount);

          JSONObject json = new JSONObject(customerCountryMap);

          System.out.println(json.toString());
        }
      }
    }
  }
}

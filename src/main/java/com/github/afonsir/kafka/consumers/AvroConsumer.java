package com.github.afonsir.kafka.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.github.afonsir.kafka.avro.Customer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class AvroConsumer {
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
      KafkaAvroDeserializer.class.getName()
    );

    props.put(
      io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
      true
    );

    props.put(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
      "http://localhost:18081"
    );

    String topicName = "customer.contacts";

    try (Consumer<String, Customer> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topicName));

      Duration timeout = Duration.ofMillis(100);

      System.out.println("Reading topic: " + topicName);

      while (true) {
        ConsumerRecords<String, Customer> records = consumer.poll(timeout);

        for (ConsumerRecord<String, Customer> record : records) {
          System.out.println("Current customer name is: " + record.value().getName());
        }

        consumer.commitSync();
      }
    }
  }
}

package com.github.afonsir.kafka.producers;

import org.apache.kafka.clients.producer.*;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import com.github.afonsir.kafka.avro.Customer;

import java.util.Properties;

public class AvroProducer {
  public static void main(String[] args) {

    Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        KafkaAvroSerializer.class.getName()
    );
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        KafkaAvroSerializer.class.getName()
    );

    props.put(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
      "http://localhost:18081"
    );

    String topicName = "customer.contacts";

    try (Producer<String, Customer> producer = new KafkaProducer<>(props)) {
      while (true) {
        Customer customer = CustomerGenerator.getNext();

        System.out.println("Generated customer " + customer.toString());

        ProducerRecord<String, Customer> record = new ProducerRecord<>(
          topicName,
          customer.getName(),
          customer
        );

        producer.send(record);

        Thread.sleep(1000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

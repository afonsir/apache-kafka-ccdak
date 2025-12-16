package com.github.afonsir.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {

  public static void main(String[] args) {

    Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");

    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName()
    );
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName()
    );

    Producer<String, String> oldProducer = new KafkaProducer<>(props);

    ProducerRecord<String, String> record = new ProducerRecord<>(
      "customer.countries",
      "Precision Products",
      "France"
    );

    try {
      oldProducer.send(record);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      oldProducer.flush();
      oldProducer.close(); 
    }

    try (Producer<String, String> newProducer = new KafkaProducer<>(props)) {
      newProducer.send(record);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

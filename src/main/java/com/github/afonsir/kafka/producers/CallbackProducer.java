package com.github.afonsir.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CallbackProducer {
  private static class CustomProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {

    Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName()
    );
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName()
    );

    ProducerRecord<String, String> record = new ProducerRecord<>(
      "customer.countries",
      "Biomedical Materials",
      "USA"
    );

    try (Producer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(record, new CustomProducerCallback());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

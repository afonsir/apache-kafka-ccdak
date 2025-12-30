package com.github.afonsir.kafka.producers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import java.util.Properties;

public class GenericAvroProducer {
  public static void main(String[] args) {

    Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:39092");

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

    int customers = 100;
    String topicName = "customer.contacts";
    String schemaString =
      """
      {
        "namespace": "com.github.afonsir.kafka.avro",
        "name": "Customer",
        "type": "record",
        "fields": [
          {
            "name": "id",
            "type": "int"
          },
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "email",
            "type": ["null", "string"],
            "default": "null"
          }
        ]
      }
      """;

    try (Producer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
      Schema.Parser parser = new Schema.Parser();
      Schema schema = parser.parse(schemaString);

      for (int nCustomers = 0; nCustomers < customers; nCustomers++) {
        String customerName = "exampleCustomer" + nCustomers;
        String customerEmail = "example_" + nCustomers + "@example.com";

        GenericRecord customer = new GenericData.Record(schema);
        customer.put("id", nCustomers);
        customer.put("name", customerName);
        customer.put("email", customerEmail);

        System.out.println("Generated customer " + customer.toString());

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(
          topicName,
          customerName,
          customer
        );

        producer.send(record);

        Thread.sleep(500);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

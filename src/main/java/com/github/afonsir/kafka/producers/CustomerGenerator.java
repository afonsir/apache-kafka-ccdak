package com.github.afonsir.kafka.producers;

import com.github.afonsir.kafka.avro.Customer;

import java.util.Random;
import java.util.UUID;

public class CustomerGenerator {

  private static final Random RANDOM = new Random();

  public static Customer getNext() {

    return Customer.newBuilder()
        .setId(randomId())
        .setName(randomName())
        .setEmail(randomEmail())
        .build();
  }

  private static String randomName() {
    return "Customer-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private static String randomEmail() {
    return "user" + RANDOM.nextInt(10_000) + "@example.com";
  }

  private static int randomId() {
    return RANDOM.nextInt(60);
  }
}

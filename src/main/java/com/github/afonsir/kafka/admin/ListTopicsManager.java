package com.github.afonsir.kafka.admin;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class ListTopicsManager {
  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

    try (AdminClient admin = AdminClient.create(props)) {
      ListTopicsResult topics = admin.listTopics();

      topics.names().get().forEach(System.out::println);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

    } catch (ExecutionException e) {
      throw new RuntimeException("Error for listing topics", e.getCause());
    }
  }
}

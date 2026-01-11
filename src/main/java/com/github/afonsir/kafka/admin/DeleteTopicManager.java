package com.github.afonsir.kafka.admin;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;

public class DeleteTopicManager {
  private static final String TOPIC_NAME = "customer.test";
  private static final Collection<String> TOPIC_LIST = List.of(
    TOPIC_NAME
  );

  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");

    try (AdminClient admin = AdminClient.create(props)) {
      admin.deleteTopics(TOPIC_LIST).all().get();

      DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);
      demoTopic.topicNameValues().get(TOPIC_NAME).get();

      System.out.println("Topic " + TOPIC_NAME + " is still around");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

    } catch (ExecutionException e) {
      System.out.println("Topic " + TOPIC_NAME + " is gone");

    }
  }
}

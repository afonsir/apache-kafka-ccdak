package com.github.afonsir.kafka.admin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

public class CreateTopicManager {
  private static final Collection<String> TOPIC_LIST = List.of(
    "customer.countries",
    "customer.contacts",
    "customer.test"
  );

  private static final String TOPIC_NAME = "customer.test";
  private static final int NUM_PARTITIONS = 3;
  private static final short REP_FACTOR = 2;

  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:39092");

    AdminClient admin = AdminClient.create(props);

    try {
      DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);
      TopicDescription topicDescription = demoTopic.topicNameValues().get(TOPIC_NAME).get();

      System.out.println("Description of demo topic: " + topicDescription);

      if (topicDescription.partitions().size() != NUM_PARTITIONS) {
        System.out.println("Topic has wrong number of partitions. Exiting.");
        System.exit(-1);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

    } catch (ExecutionException e) {
      if (! (e.getCause() instanceof UnknownTopicOrPartitionException)) {
        e.printStackTrace();

        throw new RuntimeException("Unexpected exception.");
      }

      System.out.println("Topic " + TOPIC_NAME + " does not exist. Going to create it now");

      try {
        CreateTopicsResult newTopic = admin.createTopics(
          Collections.singletonList(
            new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)
          )
        );

        if (newTopic.numPartitions(TOPIC_NAME).get() != NUM_PARTITIONS) {
          System.out.println("Topic has wrong number of partitions.");
          System.exit(-1);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();

      } catch (ExecutionException ex) {
        throw new RuntimeException("Unexpected exception.");
      }
    } finally {
      admin.close();
    }
  }
}

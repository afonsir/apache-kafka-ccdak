package com.github.afonsir.kafka.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

public class ChangeTopicManager {
  private static final String TOPIC_NAME = "customer.test";

  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

    try (AdminClient admin = AdminClient.create(props)) {
      ConfigResource configResource = new ConfigResource(
        ConfigResource.Type.TOPIC,
        TOPIC_NAME
      );

      DescribeConfigsResult configsResult = admin.describeConfigs(
        Collections.singleton(configResource)
      );

      Config configs = configsResult.all().get().get(configResource);
      configs.entries().stream().filter(entry -> !entry.isDefault()).forEach(System.out::println);

      ConfigEntry compaction = new ConfigEntry(
        TopicConfig.CLEANUP_POLICY_CONFIG,
        TopicConfig.CLEANUP_POLICY_COMPACT
      );

      ConfigEntry cleanupPolicy = configs.get(TopicConfig.CLEANUP_POLICY_CONFIG);

      if (cleanupPolicy != null &&
          cleanupPolicy.value() != null &&
          !cleanupPolicy.value().contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {

        Collection<AlterConfigOp> configOp = new ArrayList<>();
        configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));

        Map<ConfigResource, Collection<AlterConfigOp>> alterConf = new HashMap<>();
        alterConf.put(configResource, configOp);

        admin.incrementalAlterConfigs(alterConf).all().get();
      } else {
        System.out.println("Topic " + TOPIC_NAME + " is compacted topic");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

    } catch (ExecutionException e) {
      throw new RuntimeException("Error for listing topics", e.getCause());
    }
  }
}

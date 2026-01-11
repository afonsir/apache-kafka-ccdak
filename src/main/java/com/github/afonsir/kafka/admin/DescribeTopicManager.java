package com.github.afonsir.kafka.admin;

import java.util.Collections;
import java.util.Properties;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture.BiConsumer;

import io.vertx.core.Vertx;

public class DescribeTopicManager {
  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");

    AdminClient admin = AdminClient.create(props);
    Vertx app = Vertx.vertx();

    app.createHttpServer().requestHandler(request -> {
      String topic = request.getParam("topic");
      String timeout = request.getParam("timeout");
      int timeoutMs = NumberUtils.toInt(timeout, 1000);

      DescribeTopicsResult demoTopic = admin.describeTopics(
        Collections.singletonList(topic),
        new DescribeTopicsOptions().timeoutMs(timeoutMs)
      );

      demoTopic.topicNameValues().get(topic).whenComplete(
        new BiConsumer<TopicDescription, Throwable>() {
          @Override
          public void accept(
            final TopicDescription topicDescription,
            final Throwable throwable
          ) {
            if (throwable != null) {
              request.response().end(
                "Error trying to describe topic " + topic + " due to " + throwable.getMessage()
              );
            } else {
              request.response().end(topicDescription.toString());
            }
          }
        }
      );

    }).listen(8081);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutting down...");
      admin.close();
      app.close();
    }));
  }
}

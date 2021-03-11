/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.test.kafka.external;

import org.apache.flink.connectors.test.common.external.ContainerizedExternalSystem;
import org.apache.flink.connectors.test.common.utils.FlinkContainers;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Kafka external system based on {@link KafkaContainer} from <a
 * href="https://www.testcontainers.org/">Testcontainers</a>.
 *
 * <p>This external system is also integrated with a Kafka admin client for topic management.
 */
public class KafkaContainerizedExternalSystem
        implements ContainerizedExternalSystem<String, KafkaContainerizedExternalSystem> {

    private FlinkContainers flink;

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaContainerizedExternalSystem.class);

    // Kafka container and its related AdminClient
    private final KafkaContainer kafka;
    private AdminClient kafkaAdminClient;

    // Name of this external system
    public static final String NAME = "KafkaContainer";

    // Hostname of Kafka in docker network, so that Flink can find it in network easily.
    public static final String HOSTNAME = "kafka";
    public static final int PORT = 9092;
    public static final String ENTRY = HOSTNAME + ":" + PORT;

    private static final int DEFAULT_TIMEOUT = 10;

    public KafkaContainerizedExternalSystem() {
        this.kafka =
                new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.2"))
                        .withNetworkAliases(HOSTNAME);
    }

    public String getBootstrapServer() {
        return kafka.getBootstrapServers().split("://")[1];
    }

    /**
     * Create a topic in Kafka container. This is a blocking method which will wait for the result
     * of topic creation.
     *
     * @param topicName Name of the new topic
     * @param numPartitions Number of partitions
     * @param replicationFactor Number of replications
     */
    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        // Make sure Kafka container is running
        Preconditions.checkState(kafka.isRunning(), "Kafka container is not running");

        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        try {
            kafkaAdminClient
                    .createTopics(Collections.singletonList(newTopic))
                    .all()
                    .get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Cannot create topic '%s'", topicName), e);
        }
    }

    public void deleteTopic(String topicName) {
        Preconditions.checkState(kafka.isRunning(), "Kafka container is not running");
        try {
            kafkaAdminClient
                    .deleteTopics(Collections.singletonList(topicName))
                    .all()
                    .get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Cannot delete topic '%s'", topicName), e);
        }
    }

    /*------------------------------ Bind with Flink -------------------------------*/
    @Override
    public KafkaContainerizedExternalSystem withFlinkContainers(FlinkContainers flink) {
        this.flink = flink;
        kafka.dependsOn(flink.getJobManager()).withNetwork(flink.getJobManager().getNetwork());
        return this;
    }

    /*--------------------------- Lifecycle management ------------------------*/

    /**
     * External system initialization.
     *
     * <p>All preparation work should be done here because testing framework is aware of nothing
     * about the external system.
     */
    @Override
    public void startUp() {
        LOG.info("🐳 Launching Kafka on Docker...");

        // Make sure kafka container is bound with Flink containers
        if (flink == null) {
            LOG.warn(
                    "Kafka container is not bound with Flink containers. This will lead to "
                            + "network isolation between Kafka and Flink");
        }

        // Start Kafka container
        kafka.start();

        // Start a Kafka admin client for management
        Properties adminClientProps = new Properties();
        adminClientProps.put("bootstrap.servers", kafka.getBootstrapServers().split("//")[1]);
        kafkaAdminClient = AdminClient.create(adminClientProps);

        LOG.info("Kafka container started.");
    }

    @Override
    public void tearDown() {
        LOG.info("🐳 Tearing down Kafka on Docker...");
        kafkaAdminClient.close();
        kafka.stop();
    }
}

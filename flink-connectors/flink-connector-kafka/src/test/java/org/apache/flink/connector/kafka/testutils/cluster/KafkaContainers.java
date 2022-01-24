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

package org.apache.flink.connector.kafka.testutils.cluster;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

/** Kafka cluster running brokers and Zookeeper in containers. */
public class KafkaContainers implements BeforeAllCallback, AfterAllCallback {

    /** Broker ID to container. */
    private final Map<Integer, KafkaContainer> kafkaContainers;

    /**
     * Zookeeper container. Marked as nullable because single broker cluster uses embedded Zookeeper
     * in broker container.
     */
    @Nullable private final GenericContainer<?> zookeeper;

    /** Create a {@link KafkaContainersBuilder}. */
    public static KafkaContainersBuilder builder() {
        return new KafkaContainersBuilder();
    }

    KafkaContainers(
            Map<Integer, KafkaContainer> kafkaContainers, @Nullable GenericContainer<?> zookeeper) {
        this.kafkaContainers = kafkaContainers;
        this.zookeeper = zookeeper;
    }

    /** Get bootstrap servers string of the cluster. */
    public String getBootstrapServers() {
        return kafkaContainers.values().stream()
                .map(KafkaContainer::getBootstrapServers)
                .collect(Collectors.joining(","));
    }

    /** Get Kafka container by broker ID. */
    public KafkaContainer getKafkaContainer(int brokerID) {
        return kafkaContainers.get(brokerID);
    }

    /** Get all Kafka containers. */
    public Map<Integer, KafkaContainer> getKafkaContainers() {
        return kafkaContainers;
    }

    /** Get running state of the cluster. */
    public boolean isRunning() {
        return kafkaContainers.values().stream().allMatch(ContainerState::isRunning);
    }

    /** Start up the cluster. */
    public void start() {
        if (zookeeper != null) {
            zookeeper.start();
        }
        // Use ArrayList to enable parallel stream (start multiple brokers in parallel)
        new ArrayList<>(kafkaContainers.values()).parallelStream().forEach(KafkaContainer::start);
    }

    /** Stop the cluster. */
    public void stop() {
        // Use ArrayList to enable parallel stream (stop multiple brokers in parallel)
        new ArrayList<>(kafkaContainers.values()).parallelStream().forEach(KafkaContainer::stop);
        if (zookeeper != null) {
            zookeeper.stop();
        }
    }

    // ----------------------- JUnit 5 Lifecycle Management --------------------------

    @Override
    public void beforeAll(ExtensionContext context) {
        start();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        stop();
    }
}

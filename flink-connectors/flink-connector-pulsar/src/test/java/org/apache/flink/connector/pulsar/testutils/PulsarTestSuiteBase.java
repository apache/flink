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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * The base class for the all Pulsar related test sites. It brings up:
 *
 * <ul>
 *   <li>A Zookeeper cluster.
 *   <li>Pulsar Broker.
 *   <li>A Bookkeeper cluster.
 * </ul>
 *
 * <p>You just need to write a JUnit 5 test class and extends this suite class. All the helper
 * method list below would be ready.
 *
 * <p>{@code PulsarSourceEnumeratorTest} would be a test example for how to use this base class. If
 * you have some setup logic, such as create topic or send message, just place then in a setup
 * method with annotation {@code @BeforeAll}. This setup method would not require {@code static}.
 *
 * @see PulsarContainerOperator for how to use the helper methods in this class.
 */
@ExtendWith(TestLoggerExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class PulsarTestSuiteBase {

    @RegisterExtension
    final PulsarContainerEnvironment environment = new PulsarContainerEnvironment();

    // Helper method for real test classes.

    protected void setupTopic(String topic) {
        environment.operator().setupTopic(topic);
    }

    public <T> void setupTopic(String topic, Schema<T> schema, Supplier<T> supplier) {
        environment.operator().setupTopic(topic, schema, supplier);
    }

    protected void createTopic(String topic, int numberOfPartitions) {
        environment.operator().createTopic(topic, numberOfPartitions);
    }

    protected void increaseTopicPartitions(String topic, int newPartitionsNum) {
        environment.operator().increaseTopicPartitions(topic, newPartitionsNum);
    }

    protected void deleteTopic(String topic, boolean isPartitioned) {
        environment.operator().deleteTopic(topic, isPartitioned);
    }

    protected List<TopicPartition> topicInfo(String topic) {
        return environment.operator().topicInfo(topic);
    }

    protected List<TopicPartition> topicsInfo(Collection<String> topics) {
        return environment.operator().topicsInfo(topics);
    }

    protected <T> MessageId sendMessage(String topic, Schema<T> schema, T message) {
        return environment.operator().sendMessage(topic, schema, message);
    }

    protected <T> List<MessageId> sendMessages(
            String topic, Schema<T> schema, Collection<T> messages) {
        return environment.operator().sendMessages(topic, schema, messages);
    }

    protected String serviceUrl() {
        return environment.operator().serviceUrl();
    }

    protected String adminUrl() {
        return environment.operator().adminUrl();
    }

    protected PulsarClient client() {
        return environment.operator().client();
    }

    protected PulsarAdmin admin() {
        return environment.operator().admin();
    }
}

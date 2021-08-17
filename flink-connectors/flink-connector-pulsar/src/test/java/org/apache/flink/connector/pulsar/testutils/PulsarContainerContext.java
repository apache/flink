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
import org.apache.flink.connectors.test.common.external.ExternalContext;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

/** Common test context for pulsar container based test. */
public abstract class PulsarContainerContext<T> implements ExternalContext<T> {

    private static final int NUM_RECORDS_UPPER_BOUND = 500;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;

    private final String displayName;
    protected final PulsarContainerOperator operator;

    protected PulsarContainerContext(String displayName, PulsarContainerEnvironment environment) {
        this.displayName = displayName;
        this.operator = environment.operator();
    }

    // Helper methods for generating data.

    protected List<String> generateStringTestData(long seed) {
        Random random = new Random(seed);
        int recordNum =
                random.nextInt(NUM_RECORDS_UPPER_BOUND - NUM_RECORDS_LOWER_BOUND)
                        + NUM_RECORDS_LOWER_BOUND;
        List<String> records = new ArrayList<>(recordNum);

        for (int i = 0; i < recordNum; i++) {
            int stringLength = random.nextInt(50) + 1;
            records.add(randomAlphanumeric(stringLength));
        }

        return records;
    }

    // Helper methods for operating pulsar.

    protected void setupTopic(String topic) {
        operator.setupTopic(topic);
    }

    protected void setupTopic(String topic, Schema<T> schema, Supplier<T> supplier) {
        operator.setupTopic(topic, schema, supplier);
    }

    protected void createTopic(String topic, int numberOfPartitions) {
        operator.createTopic(topic, numberOfPartitions);
    }

    protected void increaseTopicPartitions(String topic, int newPartitionsNum) {
        operator.increaseTopicPartitions(topic, newPartitionsNum);
    }

    protected void deleteTopic(String topic, boolean isPartitioned) {
        operator.deleteTopic(topic, isPartitioned);
    }

    protected List<TopicPartition> topicInfo(String topic) {
        return operator.topicInfo(topic);
    }

    protected List<TopicPartition> topicsInfo(Collection<String> topics) {
        return operator.topicsInfo(topics);
    }

    protected MessageId sendMessage(String topic, Schema<T> schema, T message) {
        return operator.sendMessage(topic, schema, message);
    }

    protected List<MessageId> sendMessages(String topic, Schema<T> schema, Collection<T> messages) {
        return operator.sendMessages(topic, schema, messages);
    }

    protected String serviceUrl() {
        return operator.serviceUrl();
    }

    protected String adminUrl() {
        return operator.adminUrl();
    }

    protected PulsarClient client() {
        return operator.client();
    }

    protected PulsarAdmin admin() {
        return operator.admin();
    }

    @Override
    public String toString() {
        return displayName;
    }
}

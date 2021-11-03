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

package org.apache.flink.connector.pulsar.testutils.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connectors.test.common.external.ExternalContext;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyThrow;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A pulsar cluster operator is used for operating pulsar instance. It's serializable for using in
 * {@link ExternalContext}.
 */
public class PulsarRuntimeOperator implements Serializable, Closeable {
    private static final long serialVersionUID = -630646912412751301L;

    public static final int DEFAULT_PARTITIONS = 10;
    public static final int NUM_RECORDS_PER_PARTITION = 20;

    private String serviceUrl;
    private String adminUrl;

    private transient PulsarClient client;
    private transient PulsarAdmin admin;

    public PulsarRuntimeOperator(String serviceUrl, String adminUrl) {
        this.serviceUrl = serviceUrl;
        this.adminUrl = adminUrl;
        initializeClients();
    }

    /**
     * Create a topic with default {@link #DEFAULT_PARTITIONS} partitions and send a fixed number
     * {@link #NUM_RECORDS_PER_PARTITION} of records to this topic.
     */
    public void setupTopic(String topic) {
        Random random = new Random(System.currentTimeMillis());
        setupTopic(topic, Schema.STRING, () -> randomAlphanumeric(10 + random.nextInt(20)));
    }

    public <T> void setupTopic(String topic, Schema<T> schema, Supplier<T> supplier) {
        createTopic(topic, DEFAULT_PARTITIONS);

        // Make sure every topic partition has message.
        for (int i = 0; i < DEFAULT_PARTITIONS; i++) {
            String partitionName = TopicNameUtils.topicNameWithPartition(topic, i);
            List<T> messages =
                    Stream.generate(supplier).limit(NUM_RECORDS_PER_PARTITION).collect(toList());

            sendMessages(partitionName, schema, messages);
        }
    }

    public void createTopic(String topic, int numberOfPartitions) {
        checkArgument(numberOfPartitions >= 0);
        if (numberOfPartitions == 0) {
            createNonPartitionedTopic(topic);
        } else {
            createPartitionedTopic(topic, numberOfPartitions);
        }
    }

    public void increaseTopicPartitions(String topic, int newPartitionsNum) {
        sneakyAdmin(() -> admin().topics().updatePartitionedTopic(topic, newPartitionsNum));
    }

    public void deleteTopic(String topic, boolean isPartitioned) {
        if (isPartitioned) {
            sneakyAdmin(() -> admin().topics().deletePartitionedTopic(topic));
        } else {
            sneakyAdmin(() -> admin().topics().delete(topic));
        }
    }

    public List<TopicPartition> topicInfo(String topic) {
        try {
            return client().getPartitionsForTopic(topic).get().stream()
                    .map(
                            p ->
                                    new TopicPartition(
                                            topic,
                                            TopicName.getPartitionIndex(p),
                                            TopicRange.createFullRange()))
                    .collect(toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    protected List<TopicPartition> topicsInfo(Collection<String> topics) {
        return topics.stream().flatMap(s -> topicInfo(s).stream()).collect(toList());
    }

    public <T> MessageId sendMessage(String topic, Schema<T> schema, T message) {
        List<MessageId> messageIds = sendMessages(topic, schema, singletonList(message));
        checkArgument(messageIds.size() == 1);

        return messageIds.get(0);
    }

    public <T> MessageId sendMessage(String topic, Schema<T> schema, String key, T message) {
        List<MessageId> messageIds = sendMessages(topic, schema, key, singletonList(message));
        checkArgument(messageIds.size() == 1);

        return messageIds.get(0);
    }

    public <T> List<MessageId> sendMessages(
            String topic, Schema<T> schema, Collection<T> messages) {
        return sendMessages(topic, schema, null, messages);
    }

    public <T> List<MessageId> sendMessages(
            String topic, Schema<T> schema, String key, Collection<T> messages) {
        try (Producer<T> producer = client().newProducer(schema).topic(topic).create()) {
            List<MessageId> messageIds = new ArrayList<>(messages.size());

            for (T message : messages) {
                MessageId messageId;
                if (Strings.isNullOrEmpty(key)) {
                    messageId = producer.newMessage().value(message).send();
                } else {
                    messageId = producer.newMessage().key(key).value(message).send();
                }
                messageIds.add(messageId);
            }

            return messageIds;
        } catch (PulsarClientException e) {
            sneakyThrow(e);
            return emptyList();
        }
    }

    public String serviceUrl() {
        return serviceUrl;
    }

    public String adminUrl() {
        return adminUrl;
    }

    public PulsarClient client() {
        return client;
    }

    public PulsarAdmin admin() {
        return admin;
    }

    public Configuration config() {
        Configuration configuration = new Configuration();
        configuration.set(PULSAR_SERVICE_URL, serviceUrl());
        configuration.set(PULSAR_ADMIN_URL, adminUrl());

        return configuration;
    }

    @Override
    public void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (client != null) {
            client.close();
        }
    }

    // --------------------------- Private Methods -----------------------------

    private void createNonPartitionedTopic(String topic) {
        try {
            admin().lookups().lookupTopic(topic);
            sneakyAdmin(() -> admin().topics().expireMessagesForAllSubscriptions(topic, 0));
        } catch (PulsarAdminException e) {
            sneakyAdmin(() -> admin().topics().createNonPartitionedTopic(topic));
        }
    }

    private void createPartitionedTopic(String topic, int numberOfPartitions) {
        try {
            admin().lookups().lookupPartitionedTopic(topic);
            sneakyAdmin(() -> admin().topics().expireMessagesForAllSubscriptionsAsync(topic, 0));
        } catch (PulsarAdminException e) {
            sneakyAdmin(() -> admin().topics().createPartitionedTopic(topic, numberOfPartitions));
        }
    }

    private void initializeClients() {
        this.client = sneakyClient(() -> PulsarClient.builder().serviceUrl(serviceUrl).build());
        this.admin = sneakyClient(() -> PulsarAdmin.builder().serviceHttpUrl(adminUrl).build());
    }

    // --------------------------- Serialization Logic -----------------------------

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeUTF(serviceUrl);
        oos.writeUTF(adminUrl);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        this.serviceUrl = ois.readUTF();
        this.adminUrl = ois.readUTF();
        initializeClients();
    }
}

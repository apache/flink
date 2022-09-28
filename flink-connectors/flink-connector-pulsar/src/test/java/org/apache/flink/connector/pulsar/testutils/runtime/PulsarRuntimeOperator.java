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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.config.PulsarConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.testframe.external.ExternalContext;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ENABLE_TRANSACTION;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyThrow;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_SEND_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_WRITE_DELIVERY_GUARANTEE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_WRITE_TRANSACTION_TIMEOUT;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.pulsar.client.api.MessageId.earliest;
import static org.apache.pulsar.client.api.ProducerAccessMode.Shared;
import static org.apache.pulsar.client.api.SubscriptionMode.Durable;
import static org.apache.pulsar.client.api.SubscriptionType.Exclusive;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;

/**
 * A pulsar cluster operator used for operating pulsar instance. It's serializable for using in
 * {@link ExternalContext}.
 */
public class PulsarRuntimeOperator implements Closeable {

    public static final int DEFAULT_PARTITIONS = 10;
    public static final int NUM_RECORDS_PER_PARTITION = 20;
    public static final String SUBSCRIPTION_NAME = "PulsarRuntimeOperator";

    private final String serviceUrl;
    private final String adminUrl;
    private final PulsarClient client;
    private final PulsarAdmin admin;

    public PulsarRuntimeOperator(String serviceUrl, String adminUrl) {
        this(serviceUrl, serviceUrl, adminUrl, adminUrl);
    }

    public PulsarRuntimeOperator(
            String serviceUrl,
            String containerServiceUrl,
            String adminUrl,
            String containerAdminUrl) {
        this.serviceUrl = containerServiceUrl;
        this.adminUrl = containerAdminUrl;
        this.client =
                sneakyClient(
                        () ->
                                PulsarClient.builder()
                                        .serviceUrl(serviceUrl)
                                        .enableTransaction(true)
                                        .build());
        this.admin = sneakyClient(() -> PulsarAdmin.builder().serviceHttpUrl(adminUrl).build());
    }

    /**
     * Create a topic with default {@link #DEFAULT_PARTITIONS} partitions and send a fixed number
     * {@link #NUM_RECORDS_PER_PARTITION} of records to this topic.
     *
     * @param topic Pulsar topic name, it couldn't be a name with partition index.
     */
    public void setupTopic(String topic) {
        Random random = new Random(System.currentTimeMillis());
        setupTopic(topic, Schema.STRING, () -> randomAlphanumeric(10 + random.nextInt(20)));
    }

    /**
     * Create a topic with default {@link #DEFAULT_PARTITIONS} partitions and send a fixed number
     * {@link #NUM_RECORDS_PER_PARTITION} of records to this topic.
     *
     * @param topic Pulsar topic name, it couldn't be a name with partition index.
     * @param schema The Pulsar schema for serializing records into bytes.
     * @param supplier The supplier for providing the records which would be sent to Pulsar.
     */
    public <T> void setupTopic(String topic, Schema<T> schema, Supplier<T> supplier) {
        setupTopic(topic, schema, supplier, NUM_RECORDS_PER_PARTITION);
    }

    /**
     * Create a topic with default {@link #DEFAULT_PARTITIONS} partitions and send a fixed number of
     * records to this topic.
     *
     * @param topic Pulsar topic name, it couldn't be a name with partition index.
     * @param schema The Pulsar schema for serializing records into bytes.
     * @param supplier The supplier for providing the records which would be sent to Pulsar.
     * @param numRecordsPerSplit The number of records for a partition.
     */
    public <T> void setupTopic(
            String topic, Schema<T> schema, Supplier<T> supplier, int numRecordsPerSplit) {
        String topicName = topicName(topic);
        createTopic(topicName, DEFAULT_PARTITIONS);

        // Make sure every topic partition has messages.
        for (int i = 0; i < DEFAULT_PARTITIONS; i++) {
            String partitionName = topicNameWithPartition(topic, i);
            List<T> messages =
                    Stream.generate(supplier).limit(numRecordsPerSplit).collect(toList());

            sendMessages(partitionName, schema, messages);
        }
    }

    /**
     * Create a pulsar topic with given partition number if the topic doesn't exist. We won't do
     * anything for the existing topic. Make sure correctly used in the testing code.
     *
     * @param topic The name of the topic.
     * @param numberOfPartitions The number of partitions. We would create a non-partitioned topic
     *     if this number is zero.
     */
    public void createTopic(String topic, int numberOfPartitions) {
        checkArgument(numberOfPartitions >= 0);
        if (numberOfPartitions == 0) {
            createNonPartitionedTopic(topic);
        } else {
            createPartitionedTopic(topic, numberOfPartitions);
        }
    }

    /**
     * Increase the partition number of the topic.
     *
     * @param topic The topic name.
     * @param newPartitionsNum The new partition size which should exceed previous size.
     */
    public void increaseTopicPartitions(String topic, int newPartitionsNum) {
        PartitionedTopicMetadata metadata =
                sneakyAdmin(() -> admin().topics().getPartitionedTopicMetadata(topic));
        checkArgument(
                metadata.partitions < newPartitionsNum,
                "The new partition size which should greater than previous size.");

        sneakyAdmin(() -> admin().topics().updatePartitionedTopic(topic, newPartitionsNum));
    }

    /**
     * Delete a Pulsar topic.
     *
     * @param topic The topic name.
     */
    public void deleteTopic(String topic) {
        String topicName = topicName(topic);
        PartitionedTopicMetadata metadata;

        try {
            metadata = admin().topics().getPartitionedTopicMetadata(topicName);
        } catch (NotFoundException e) {
            // This topic doesn't exist. Just skip deletion.
            return;
        } catch (PulsarAdminException e) {
            sneakyThrow(e);
            return;
        }

        if (metadata.partitions == NON_PARTITIONED) {
            sneakyAdmin(() -> admin().topics().delete(topicName));
        } else {
            sneakyAdmin(() -> admin().topics().deletePartitionedTopic(topicName));
        }
    }

    /** Convert the topic metadata into a list of topic partitions. */
    public List<TopicPartition> topicInfo(String topic) {
        try {
            return client().getPartitionsForTopic(topic).get().stream()
                    .map(p -> new TopicPartition(topic, TopicName.getPartitionIndex(p)))
                    .collect(toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Send a single message to Pulsar, return the message id after the ack from Pulsar.
     *
     * @param topic The name of the topic.
     * @param schema The schema for serialization.
     * @param message The record need to be sent.
     * @param <T> The type of the record.
     * @return message id.
     */
    public <T> MessageId sendMessage(String topic, Schema<T> schema, T message) {
        List<MessageId> messageIds = sendMessages(topic, schema, singletonList(message));
        checkArgument(messageIds.size() == 1);

        return messageIds.get(0);
    }

    /**
     * Send a single message to Pulsar, return the message id after the ack from Pulsar.
     *
     * @param topic The name of the topic.
     * @param schema The schema for serialization.
     * @param key The message key.
     * @param message The record need to be sent.
     * @param <T> The type of the record.
     * @return message id.
     */
    public <T> MessageId sendMessage(String topic, Schema<T> schema, String key, T message) {
        List<MessageId> messageIds = sendMessages(topic, schema, key, singletonList(message));
        checkArgument(messageIds.size() == 1);

        return messageIds.get(0);
    }

    /**
     * Send a list of messages to Pulsar, return the message id set after the ack from Pulsar.
     *
     * @param topic The name of the topic.
     * @param schema The schema for serialization.
     * @param messages The records need to be sent.
     * @param <T> The type of the record.
     * @return message id.
     */
    public <T> List<MessageId> sendMessages(
            String topic, Schema<T> schema, Collection<T> messages) {
        return sendMessages(topic, schema, null, messages);
    }

    /**
     * Send a list messages to Pulsar, return the message id set after the ack from Pulsar.
     *
     * @param topic The name of the topic.
     * @param schema The schema for serialization.
     * @param key The message key.
     * @param messages The records need to be sent.
     * @param <T> The type of the record.
     * @return message id.
     */
    public <T> List<MessageId> sendMessages(
            String topic, Schema<T> schema, String key, Collection<T> messages) {
        try (Producer<T> producer = createProducer(topic, schema)) {
            List<MessageId> messageIds = new ArrayList<>(messages.size());

            for (T message : messages) {
                TypedMessageBuilder<T> builder = producer.newMessage().value(message);
                if (!Strings.isNullOrEmpty(key)) {
                    builder.key(key);
                }
                MessageId messageId = builder.send();
                messageIds.add(messageId);
            }
            producer.flush();
            return messageIds;
        } catch (PulsarClientException e) {
            sneakyThrow(e);
            return emptyList();
        }
    }

    /**
     * Consume a message from the given Pulsar topic, this method would be blocked until we get a
     * message from this topic.
     */
    public <T> Message<T> receiveMessage(String topic, Schema<T> schema) {
        try (Consumer<T> consumer = createConsumer(topic, schema)) {
            Message<T> message = consumer.receive();
            consumer.acknowledge(message.getMessageId());
            return message;
        } catch (PulsarClientException e) {
            sneakyThrow(e);
            return null;
        }
    }

    /**
     * Consume a message from the given Pulsar topic, this method would be blocked until we meet
     * timeout. A null message would be returned if no message has been consumed from Pulsar.
     */
    public <T> Message<T> receiveMessage(String topic, Schema<T> schema, Duration timeout) {
        try (Consumer<T> consumer = createConsumer(topic, schema)) {
            Message<T> message =
                    consumer.receive(Math.toIntExact(timeout.toMillis()), MILLISECONDS);
            consumer.acknowledge(message.getMessageId());

            return message;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Consume a fixed number of messages from the given Pulsar topic, this method would be blocked
     * until we get the exactly number of messages from this topic.
     */
    public <T> List<Message<T>> receiveMessages(String topic, Schema<T> schema, int counts) {
        if (counts == 0) {
            return emptyList();
        } else if (counts < 0) {
            // Drain all messages.
            return receiveAllMessages(topic, schema, Duration.ofMinutes(1));
        } else if (counts == 1) {
            // Drain one message.
            Message<T> message = receiveMessage(topic, schema);
            return singletonList(message);
        } else {
            // Drain a fixed number of messages.
            try (Consumer<T> consumer = createConsumer(topic, schema)) {
                List<Message<T>> messages = new ArrayList<>(counts);
                for (int i = 0; i < counts; i++) {
                    Message<T> message = consumer.receive();
                    messages.add(message);
                    consumer.acknowledge(message.getMessageId());
                }
                return messages;
            } catch (PulsarClientException e) {
                sneakyThrow(e);
                return emptyList();
            }
        }
    }

    /**
     * Drain all the messages from current topic. We will wait for all the messages has been
     * consumed until the timeout.
     */
    public <T> List<Message<T>> receiveAllMessages(
            String topic, Schema<T> schema, Duration timeout) {
        List<Message<T>> messages = new ArrayList<>();

        Message<T> message = receiveMessage(topic, schema, timeout);
        while (message != null) {
            messages.add(message);
            message = receiveMessage(topic, schema, timeout);
        }

        return messages;
    }

    /** Return the transaction coordinator client for operating {@link TxnID}. */
    public TransactionCoordinatorClient coordinatorClient() {
        return ((PulsarClientImpl) client()).getTcClient();
    }

    /**
     * Return the broker url for this Pulsar runtime. It's only used in flink environment. You can't
     * create the {@link PulsarClient} by this broker url, use the {@link #client()} instead.
     */
    public String serviceUrl() {
        return serviceUrl;
    }

    /**
     * Return the broker http url for this Pulsar runtime. It's only used in flink environment. You
     * can't create the {@link PulsarAdmin} by this broker http url, use the {@link #admin()}
     * instead.
     */
    public String adminUrl() {
        return adminUrl;
    }

    /** The client for creating producer and consumer. It's used in tests. */
    public PulsarClient client() {
        return client;
    }

    /** The client for creating topics and query other metadata, etc. It's used in tests. */
    public PulsarAdmin admin() {
        return admin;
    }

    /** The configuration for constructing {@link PulsarConfiguration}. */
    public Configuration config() {
        Configuration configuration = new Configuration();
        configuration.set(PULSAR_SERVICE_URL, serviceUrl());
        configuration.set(PULSAR_ADMIN_URL, adminUrl());
        return configuration;
    }

    /** Create the sink configuration with common settings. */
    public Configuration sinkConfig(DeliveryGuarantee deliveryGuarantee) {
        Configuration configuration = config();
        configuration.set(PULSAR_WRITE_DELIVERY_GUARANTEE, deliveryGuarantee);
        if (deliveryGuarantee == EXACTLY_ONCE) {
            configuration.set(PULSAR_WRITE_TRANSACTION_TIMEOUT, Duration.ofMinutes(5).toMillis());
            configuration.set(PULSAR_ENABLE_TRANSACTION, true);
            configuration.set(PULSAR_SEND_TIMEOUT_MS, 0L);
        }

        return configuration;
    }

    /** This method is used for test framework. You can't close this operator manually. */
    @Override
    public void close() throws PulsarClientException {
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
            admin().topics().createNonPartitionedTopic(topic);
        } catch (PulsarAdminException e) {
            if (!(e instanceof ConflictException
                    && e.getMessage().equals("This topic already exists"))) {
                sneakyThrow(e);
            }
        }
    }

    private void createPartitionedTopic(String topic, int numberOfPartitions) {
        try {
            admin().topics().createPartitionedTopic(topic, numberOfPartitions);
        } catch (PulsarAdminException e) {
            if (!(e instanceof ConflictException
                    && e.getMessage().equals("This topic already exists"))) {
                sneakyThrow(e);
            }
        }
    }

    private <T> Producer<T> createProducer(String topic, Schema<T> schema) {
        ProducerBuilder<T> builder =
                client().newProducer(schema)
                        .topic(topic)
                        .enableBatching(false)
                        .enableMultiSchema(true)
                        .accessMode(Shared);

        return sneakyClient(builder::create);
    }

    private <T> Consumer<T> createConsumer(String topic, Schema<T> schema) {
        // Create the earliest subscription if it's not existed.
        List<String> subscriptions = sneakyAdmin(() -> admin().topics().getSubscriptions(topic));
        if (!subscriptions.contains(SUBSCRIPTION_NAME)) {
            sneakyAdmin(
                    () -> admin().topics().createSubscription(topic, SUBSCRIPTION_NAME, earliest));
        }

        // Create the consumer without the initial position.
        ConsumerBuilder<T> builder =
                client().newConsumer(schema)
                        .topic(topic)
                        .subscriptionName(SUBSCRIPTION_NAME)
                        .subscriptionMode(Durable)
                        .subscriptionType(Exclusive);

        return sneakyClient(builder::subscribe);
    }
}

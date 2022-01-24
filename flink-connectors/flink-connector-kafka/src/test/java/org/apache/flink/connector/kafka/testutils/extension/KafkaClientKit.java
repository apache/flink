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

package org.apache.flink.connector.kafka.testutils.extension;

import org.apache.flink.connector.kafka.testutils.annotations.KafkaKit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Toolkit for interacting with brokers started by {@link KafkaExtension}.
 *
 * <p>Multiple ways are provided for using this toolkit in tests:
 *
 * <ol>
 *   <li>As method parameter:
 *       <pre>
 * {@literal @}Kafka
 * public class KafkaTest {
 *     {@literal @}BeforeAll
 *     static void setup(KafkaClientKit client) {
 *         client.produceToKafka(...);
 *     }
 *
 *     {@literal @}BeforeEach
 *     void before(KafkaClientKit client) {
 *         client.produceToKafka(..)
 *     }
 *
 *     {@literal @}Test
 *     void test(KafkaClientKit client) {
 *       client.createConsumer(...);
 *     }
 * }</pre>
 *   <li>As class field together with annotation {@link KafkaKit}, if many interactions with Kafka
 *       appear in the test class:
 *       <pre>
 * {@literal @}Kafka
 * public class KafkaTest {
 *
 *     // As static field
 *     {@literal @}KafkaKit static KafkaClientKit staticClient;
 *
 *     // As non-static field
 *     {@literal @}KafkaKit KafkaClientKit client;
 *
 *     {@literal @}BeforeAll
 *     static void setup() {
 *         staticClient.produceToKafka(...);
 *     }
 *
 *     {@literal @}Test
 *     void test() {
 *         client.createConsumer(...);
 *     }
 * }</pre>
 * </ol>
 */
public class KafkaClientKit {

    public static final Duration DEFAULT_TIMEOUT = KafkaExtension.DEFAULT_TIMEOUT;
    public static final int DEFAULT_NUM_PARTITIONS = 10;
    public static final short DEFAULT_REPLICATION_FACTOR = 1;

    private final String bootstrapServers;
    private final AdminClient adminClient;

    public KafkaClientKit(String bootstrapServers, AdminClient adminClient) {
        this.bootstrapServers = bootstrapServers;
        this.adminClient = adminClient;
    }

    // -------------------  Admin Client ---------------------

    /** Get bootstrap servers of the Kafka cluster. */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Get admin client connected to the Kafka cluster.
     *
     * <p>NOTE: Please do not close this admin client because it is reused across test cases.
     */
    public AdminClient getAdminClient() {
        return adminClient;
    }

    /** Create an instance of admin client. */
    public AdminClient createAdminClient() {
        return AdminClient.create(createAdminClientProps());
    }

    /** Create properties for admin client. */
    public Properties createAdminClientProps() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

    // -------------------  Kafka Consumer -------------------

    /** Create an instance of Kafka consumer with random group ID and given deserializers. */
    public <K, V> KafkaConsumer<K, V> createConsumer(
            Class<? extends Deserializer<K>> keyDeserializer,
            Class<? extends Deserializer<V>> valueDeserializer) {
        return createConsumer(UUID.randomUUID().toString(), keyDeserializer, valueDeserializer);
    }

    /** Create an instance of Kafka consumer with given configurations. */
    public <K, V> KafkaConsumer<K, V> createConsumer(
            String groupId,
            Class<? extends Deserializer<K>> keyDeserializer,
            Class<? extends Deserializer<V>> valueDeserializer) {
        Properties props = createConsumerProps(groupId, keyDeserializer, valueDeserializer);
        return new KafkaConsumer<>(props);
    }

    /** Create properties for Kafka consumer. */
    public <K, V> Properties createConsumerProps(
            Class<? extends Deserializer<K>> keyDeserializer,
            Class<? extends Deserializer<V>> valueDeserializer) {
        return createConsumerProps(
                UUID.randomUUID().toString(), keyDeserializer, valueDeserializer);
    }

    /** Create properties for Kafka consumer. */
    public <K, V> Properties createConsumerProps(
            String groupId,
            Class<? extends Deserializer<K>> keyDeserializer,
            Class<? extends Deserializer<V>> valueDeserializer) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getCanonicalName());
        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                valueDeserializer.getCanonicalName());
        return props;
    }

    // -------------------  Kafka Producer -------------------

    /** Create an instance of Kafka producer with given configurations. */
    public <K, V> KafkaProducer<K, V> createProducer(
            Class<? extends Serializer<K>> keySerializer,
            Class<? extends Serializer<V>> valueSerializer) {
        Properties props = createProducerProps(keySerializer, valueSerializer);
        return new KafkaProducer<>(props);
    }

    /** Create properties for Kafka producer. */
    public <K, V> Properties createProducerProps(
            Class<? extends Serializer<K>> keySerializer,
            Class<? extends Serializer<V>> valueSerializer) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getCanonicalName());
        props.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getCanonicalName());
        return props;
    }

    // ------------------ Topic Manipulations -----------------

    /** Create a topic with default configs on Kafka cluster. */
    public void createTopic(String name) throws Exception {
        createTopic(name, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR, DEFAULT_TIMEOUT);
    }

    /** Create a topic with specific number of partitions and replication refactor. */
    public void createTopic(String name, int numPartitions, short replicationRefactor)
            throws Exception {
        createTopic(name, numPartitions, replicationRefactor, DEFAULT_TIMEOUT);
    }

    /** Create a topic with specific number of partitions, replication refactor and timeout. */
    public void createTopic(
            String name, int numPartitions, short replicationRefactor, Duration timeout)
            throws Exception {
        adminClient
                .createTopics(
                        Collections.singleton(
                                new NewTopic(name, numPartitions, replicationRefactor)))
                .all()
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Delete a topic. */
    public void deleteTopic(String topic) throws Exception {
        adminClient
                .deleteTopics(Collections.singleton(topic))
                .all()
                .get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Produce records to Kafka. */
    public <K, V> void produceToKafka(
            List<ProducerRecord<K, V>> records,
            Class<? extends Serializer<K>> keySerializer,
            Class<? extends Serializer<V>> valueSerializer)
            throws Exception {
        produceToKafka(records, createProducerProps(keySerializer, valueSerializer));
    }

    /** Produce records to Kafka with specific producer properties. */
    public <K, V> void produceToKafka(List<ProducerRecord<K, V>> records, Properties producerProps)
            throws Exception {
        AtomicReference<Exception> sendingError = new AtomicReference<>();
        Callback callback =
                (metadata, exception) -> {
                    if (exception != null) {
                        if (!sendingError.compareAndSet(null, exception)) {
                            sendingError.get().addSuppressed(exception);
                        }
                    }
                };
        try (KafkaProducer<K, V> producer = new KafkaProducer<>(producerProps)) {
            for (ProducerRecord<K, V> record : records) {
                producer.send(record, callback);
            }
            producer.flush();
        }
        if (sendingError.get() != null) {
            throw sendingError.get();
        }
    }

    /** Get all {@link TopicPartition}s in given topics. */
    public Set<TopicPartition> getPartitionsForTopics(String... topic) throws Exception {
        return getPartitionsForTopics(Arrays.asList(topic));
    }

    /** Get all {@link TopicPartition}s in given topics. */
    public Set<TopicPartition> getPartitionsForTopics(Collection<String> topics) throws Exception {
        Set<TopicPartition> partitions = new HashSet<>();
        Map<String, TopicDescription> topicDescriptions =
                adminClient
                        .describeTopics(topics)
                        .all()
                        .get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        for (Map.Entry<String, TopicDescription> topicDescription : topicDescriptions.entrySet()) {
            final String topic = topicDescription.getKey();
            final TopicDescription description = topicDescription.getValue();
            for (int partition = 0; partition < description.partitions().size(); partition++) {
                partitions.add(new TopicPartition(topic, partition));
            }
        }
        return partitions;
    }

    /**
     * Set earliest offsets of the given topic.
     *
     * <p>Note that this method is implemented by deleting records before the given offset, so
     * please make sure records in the topic have been written beyond the specified offsets.
     *
     * @param topic Topic name to setup offsets
     * @param offsetsSetter Function for setting offset of the given {@link TopicPartition}
     */
    public void setEarliestOffsets(String topic, Function<TopicPartition, Long> offsetsSetter)
            throws Exception {
        Map<TopicPartition, RecordsToDelete> toDelete = new HashMap<>();
        for (TopicPartition tp : getPartitionsForTopics(topic)) {
            Long offset = offsetsSetter.apply(tp);
            toDelete.put(tp, RecordsToDelete.beforeOffset(offset));
            adminClient
                    .deleteRecords(toDelete)
                    .all()
                    .get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Set committed offsets of the given topic.
     *
     * <p>Please make sure records in the topic have been written beyond the specified offsets.
     *
     * @param topic Topic name to setup offsets
     * @param offsetsSetter Function for setting offset of the given {@link TopicPartition}
     */
    public void setCommittedOffsets(
            String topic, String groupId, Function<TopicPartition, Long> offsetsSetter)
            throws Exception {
        try (KafkaConsumer<String, String> consumer =
                createConsumer(groupId, StringDeserializer.class, StringDeserializer.class)) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
            for (TopicPartition tp : getPartitionsForTopics(Collections.singleton(topic))) {
                committedOffsets.put(tp, new OffsetAndMetadata(offsetsSetter.apply(tp)));
            }
            consumer.commitSync(committedOffsets);
            Map<TopicPartition, OffsetAndMetadata> toVerify =
                    adminClient
                            .listConsumerGroupOffsets(
                                    groupId,
                                    new ListConsumerGroupOffsetsOptions()
                                            .topicPartitions(
                                                    new ArrayList<>(committedOffsets.keySet())))
                            .partitionsToOffsetAndMetadata()
                            .get();
            Assertions.assertThat(toVerify)
                    .as("The offsets are not committed")
                    .isEqualTo(committedOffsets);
        }
    }
}

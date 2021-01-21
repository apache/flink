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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/** The enumerator class for Kafka source. */
@Internal
public class KafkaSourceEnumerator
        implements SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceEnumerator.class);
    private final KafkaSubscriber subscriber;
    private final OffsetsInitializer startingOffsetInitializer;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final Properties properties;
    private final long partitionDiscoveryIntervalMs;
    private final SplitEnumeratorContext<KafkaPartitionSplit> context;

    // The internal states of the enumerator.
    /**
     * This set is only accessed by the partition discovery callable in the callAsync() method, i.e
     * worker thread.
     */
    private final Set<TopicPartition> discoveredPartitions;
    /** The current assignment by reader id. Only accessed by the coordinator thread. */
    private final Map<Integer, Set<KafkaPartitionSplit>> readerIdToSplitAssignments;
    /**
     * The discovered and initialized partition splits that are waiting for owner reader to be
     * ready.
     */
    private final Map<Integer, Set<KafkaPartitionSplit>> pendingPartitionSplitAssignment;
    /** The consumer group id used for this KafkaSource. */
    private final String consumerGroupId;

    // Lazily instantiated or mutable fields.
    private KafkaConsumer<byte[], byte[]> consumer;
    private AdminClient adminClient;
    private boolean noMoreNewPartitionSplits = false;

    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context) {
        this(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                new HashMap<>());
    }

    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Map<Integer, Set<KafkaPartitionSplit>> currentSplitsAssignments) {
        this.subscriber = subscriber;
        this.startingOffsetInitializer = startingOffsetInitializer;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.properties = properties;
        this.context = context;

        this.discoveredPartitions = new HashSet<>();
        this.readerIdToSplitAssignments = new HashMap<>(currentSplitsAssignments);
        this.readerIdToSplitAssignments.forEach(
                (reader, splits) ->
                        splits.forEach(s -> discoveredPartitions.add(s.getTopicPartition())));
        this.pendingPartitionSplitAssignment = new HashMap<>();
        this.partitionDiscoveryIntervalMs =
                KafkaSourceOptions.getOption(
                        properties,
                        KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS,
                        Long::parseLong);
        this.consumerGroupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    @Override
    public void start() {
        consumer = getKafkaConsumer();
        adminClient = getKafkaAdminClient();
        if (partitionDiscoveryIntervalMs > 0) {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "with partition discovery interval of {} ms.",
                    consumerGroupId,
                    partitionDiscoveryIntervalMs);
            context.callAsync(
                    this::discoverAndInitializePartitionSplit,
                    this::handlePartitionSplitChanges,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    consumerGroupId);
            context.callAsync(
                    this::discoverAndInitializePartitionSplit, this::handlePartitionSplitChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the kafka source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void addSplitsBack(List<KafkaPartitionSplit> splits, int subtaskId) {
        addPartitionSplitChangeToPendingAssignments(splits);
        assignPendingPartitionSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to KafkaSourceEnumerator for consumer group {}.",
                subtaskId,
                consumerGroupId);
        assignPendingPartitionSplits();
    }

    @Override
    public KafkaSourceEnumState snapshotState() throws Exception {
        return new KafkaSourceEnumState(readerIdToSplitAssignments);
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    // ----------------- private methods -------------------

    private PartitionSplitChange discoverAndInitializePartitionSplit() {
        // Make a copy of the partitions to owners
        KafkaSubscriber.PartitionChange partitionChange =
                subscriber.getPartitionChanges(
                        adminClient, Collections.unmodifiableSet(discoveredPartitions));

        Set<TopicPartition> newPartitions =
                Collections.unmodifiableSet(partitionChange.getNewPartitions());
        OffsetsInitializer.PartitionOffsetsRetriever offsetsRetriever = getOffsetsRetriever();

        Map<TopicPartition, Long> startingOffsets =
                startingOffsetInitializer.getPartitionOffsets(newPartitions, offsetsRetriever);
        Map<TopicPartition, Long> stoppingOffsets =
                stoppingOffsetInitializer.getPartitionOffsets(newPartitions, offsetsRetriever);

        Set<KafkaPartitionSplit> partitionSplits = new HashSet<>(newPartitions.size());
        for (TopicPartition tp : newPartitions) {
            Long startingOffset = startingOffsets.get(tp);
            long stoppingOffset =
                    stoppingOffsets.getOrDefault(tp, KafkaPartitionSplit.NO_STOPPING_OFFSET);
            partitionSplits.add(new KafkaPartitionSplit(tp, startingOffset, stoppingOffset));
        }
        discoveredPartitions.addAll(newPartitions);
        return new PartitionSplitChange(partitionSplits, partitionChange.getRemovedPartitions());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handlePartitionSplitChanges(
            PartitionSplitChange partitionSplitChange, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to handle partition splits change due to ", t);
        }
        if (partitionDiscoveryIntervalMs < 0) {
            LOG.debug("");
            noMoreNewPartitionSplits = true;
        }
        // TODO: Handle removed partitions.
        addPartitionSplitChangeToPendingAssignments(partitionSplitChange.newPartitionSplits);
        assignPendingPartitionSplits();
    }

    // This method should only be invoked in the coordinator executor thread.
    private void addPartitionSplitChangeToPendingAssignments(
            Collection<KafkaPartitionSplit> newPartitionSplits) {
        int numReaders = context.currentParallelism();
        for (KafkaPartitionSplit split : newPartitionSplits) {
            int ownerReader = getSplitOwner(split.getTopicPartition(), numReaders);
            pendingPartitionSplitAssignment
                    .computeIfAbsent(ownerReader, r -> new HashSet<>())
                    .add(split);
        }
        LOG.debug(
                "Assigned {} to {} readers of consumer group {}.",
                newPartitionSplits,
                numReaders,
                consumerGroupId);
    }

    // This method should only be invoked in the coordinator executor thread.
    private void assignPendingPartitionSplits() {
        Map<Integer, List<KafkaPartitionSplit>> incrementalAssignment = new HashMap<>();
        pendingPartitionSplitAssignment.forEach(
                (ownerReader, pendingSplits) -> {
                    if (!pendingSplits.isEmpty()
                            && context.registeredReaders().containsKey(ownerReader)) {
                        // The owner reader is ready, assign the split to the owner reader.
                        incrementalAssignment
                                .computeIfAbsent(ownerReader, r -> new ArrayList<>())
                                .addAll(pendingSplits);
                    }
                });
        if (incrementalAssignment.isEmpty()) {
            // No assignment is made.
            return;
        }

        LOG.info("Assigning splits to readers {}", incrementalAssignment);
        context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        incrementalAssignment.forEach(
                (readerOwner, newPartitionSplits) -> {
                    // Update the split assignment.
                    readerIdToSplitAssignments
                            .computeIfAbsent(readerOwner, r -> new HashSet<>())
                            .addAll(newPartitionSplits);
                    // Clear the pending splits for the reader owner.
                    pendingPartitionSplitAssignment.remove(readerOwner);
                    // Sends NoMoreSplitsEvent to the readers if there is no more partition splits
                    // to be assigned.
                    if (noMoreNewPartitionSplits) {
                        LOG.debug(
                                "No more KafkaPartitionSplits to assign. Sending NoMoreSplitsEvent to the readers "
                                        + "in consumer group {}.",
                                consumerGroupId);
                        context.signalNoMoreSplits(readerOwner);
                    }
                });
    }

    private KafkaConsumer<byte[], byte[]> getKafkaConsumer() {
        Properties consumerProps = new Properties();
        deepCopyProperties(properties, consumerProps);
        // set client id prefix
        String clientIdPrefix =
                consumerProps.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        consumerProps.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-enumerator-consumer");
        consumerProps.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        consumerProps.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        // Disable auto topic creation.
        consumerProps.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        return new KafkaConsumer<>(consumerProps);
    }

    private AdminClient getKafkaAdminClient() {
        Properties adminClientProps = new Properties();
        deepCopyProperties(properties, adminClientProps);
        // set client id prefix
        String clientIdPrefix =
                adminClientProps.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        adminClientProps.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-enumerator-admin-client");
        return AdminClient.create(adminClientProps);
    }

    private OffsetsInitializer.PartitionOffsetsRetriever getOffsetsRetriever() {
        String groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        return new PartitionOffsetsRetrieverImpl(consumer, adminClient, groupId);
    }

    /**
     * Returns the index of the target subtask that a specific Kafka partition should be assigned
     * to.
     *
     * <p>The resulting distribution of partitions of a single topic has the following contract:
     *
     * <ul>
     *   <li>1. Uniformly distributed across subtasks
     *   <li>2. Partitions are round-robin distributed (strictly clockwise w.r.t. ascending subtask
     *       indices) by using the partition id as the offset from a starting index (i.e., the index
     *       of the subtask which partition 0 of the topic will be assigned to, determined using the
     *       topic name).
     * </ul>
     *
     * @param tp the Kafka partition to assign.
     * @param numReaders the total number of readers.
     * @return the id of the subtask that owns the split.
     */
    @VisibleForTesting
    static int getSplitOwner(TopicPartition tp, int numReaders) {
        int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;

        // here, the assumption is that the id of Kafka partitions are always ascending
        // starting from 0, and therefore can be used directly as the offset clockwise from the
        // start index
        return (startIndex + tp.partition()) % numReaders;
    }

    @VisibleForTesting
    static void deepCopyProperties(Properties from, Properties to) {
        for (String key : from.stringPropertyNames()) {
            to.setProperty(key, from.getProperty(key));
        }
    }

    // --------------- private class ---------------

    private static class PartitionSplitChange {
        private final Set<KafkaPartitionSplit> newPartitionSplits;
        private final Set<TopicPartition> removedPartitions;

        private PartitionSplitChange(
                Set<KafkaPartitionSplit> newPartitionSplits,
                Set<TopicPartition> removedPartitions) {
            this.newPartitionSplits = Collections.unmodifiableSet(newPartitionSplits);
            this.removedPartitions = Collections.unmodifiableSet(removedPartitions);
        }
    }

    /** The implementation for offsets retriever with a consumer and an admin client. */
    @VisibleForTesting
    public static class PartitionOffsetsRetrieverImpl
            implements OffsetsInitializer.PartitionOffsetsRetriever, AutoCloseable {
        private final KafkaConsumer<?, ?> consumer;
        private final AdminClient adminClient;
        private final String groupId;

        public PartitionOffsetsRetrieverImpl(
                KafkaConsumer<?, ?> consumer, AdminClient adminClient, String groupId) {
            this.consumer = consumer;
            this.adminClient = adminClient;
            this.groupId = groupId;
        }

        @Override
        public Map<TopicPartition, Long> committedOffsets(Collection<TopicPartition> partitions) {
            ListConsumerGroupOffsetsOptions options =
                    new ListConsumerGroupOffsetsOptions()
                            .topicPartitions(new ArrayList<>(partitions));
            try {
                return adminClient
                        .listConsumerGroupOffsets(groupId, options)
                        .partitionsToOffsetAndMetadata()
                        .thenApply(
                                result -> {
                                    Map<TopicPartition, Long> offsets = new HashMap<>();
                                    result.forEach((tp, oam) -> offsets.put(tp, oam.offset()));
                                    return offsets;
                                })
                        .get();
            } catch (InterruptedException e) {
                throw new FlinkRuntimeException(
                        "Interrupted while listing offsets for consumer group " + groupId, e);
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException(
                        "Failed to fetch committed offsets for consumer group "
                                + groupId
                                + " due to",
                        e);
            }
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            return consumer.endOffsets(partitions);
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            return consumer.beginningOffsets(partitions);
        }

        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                Map<TopicPartition, Long> timestampsToSearch) {
            return consumer.offsetsForTimes(timestampsToSearch);
        }

        @Override
        public void close() throws Exception {
            consumer.close(Duration.ZERO);
            adminClient.close(Duration.ZERO);
        }
    }
}

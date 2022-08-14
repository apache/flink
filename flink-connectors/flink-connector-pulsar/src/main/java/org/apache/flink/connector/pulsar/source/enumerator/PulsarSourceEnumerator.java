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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.assigner.SplitAssigner;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.CursorPosition;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils.createAdmin;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;

/** The enumerator class for pulsar source. */
@Internal
public class PulsarSourceEnumerator
        implements SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceEnumerator.class);

    private final PulsarAdmin pulsarAdmin;
    private final PulsarSubscriber subscriber;
    private final StartCursor startCursor;
    private final RangeGenerator rangeGenerator;
    private final Configuration configuration;
    private final SourceConfiguration sourceConfiguration;
    private final SplitEnumeratorContext<PulsarPartitionSplit> context;
    private final SplitAssigner splitAssigner;

    public PulsarSourceEnumerator(
            PulsarSubscriber subscriber,
            StartCursor startCursor,
            RangeGenerator rangeGenerator,
            Configuration configuration,
            SourceConfiguration sourceConfiguration,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            SplitAssigner splitAssigner) {
        this.pulsarAdmin = createAdmin(configuration);
        this.subscriber = subscriber;
        this.startCursor = startCursor;
        this.rangeGenerator = rangeGenerator;
        this.configuration = configuration;
        this.sourceConfiguration = sourceConfiguration;
        this.context = context;
        this.splitAssigner = splitAssigner;
    }

    @Override
    public void start() {
        rangeGenerator.open(configuration, sourceConfiguration);

        // Check the pulsar topic information and convert it into source split.
        if (sourceConfiguration.isEnablePartitionDiscovery()) {
            LOG.info(
                    "Starting the PulsarSourceEnumerator for subscription {} "
                            + "with partition discovery interval of {} ms.",
                    sourceConfiguration.getSubscriptionDesc(),
                    sourceConfiguration.getPartitionDiscoveryIntervalMs());
            context.callAsync(
                    this::getSubscribedTopicPartitions,
                    this::checkPartitionChanges,
                    0,
                    sourceConfiguration.getPartitionDiscoveryIntervalMs());
        } else {
            LOG.info(
                    "Starting the PulsarSourceEnumerator for subscription {} "
                            + "without periodic partition discovery.",
                    sourceConfiguration.getSubscriptionDesc());
            context.callAsync(this::getSubscribedTopicPartitions, this::checkPartitionChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the pulsar source pushes splits eagerly, rather than act upon split requests.
    }

    @Override
    public void addSplitsBack(List<PulsarPartitionSplit> splits, int subtaskId) {
        // Put the split back to current pending splits.
        splitAssigner.addSplitsBack(splits, subtaskId);

        // If the failed subtask has already restarted, we need to assign pending splits to it.
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingPartitionSplits(singletonList(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to PulsarSourceEnumerator for subscription {}.",
                subtaskId,
                sourceConfiguration.getSubscriptionDesc());
        assignPendingPartitionSplits(singletonList(subtaskId));
    }

    @Override
    public PulsarSourceEnumState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState();
    }

    @Override
    public void close() {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

    // ----------------- private methods -------------------

    /**
     * List subscribed topic partitions on Pulsar cluster.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * requires network I/O with Pulsar cluster.
     *
     * @return Set of subscribed {@link TopicPartition}s
     */
    private Set<TopicPartition> getSubscribedTopicPartitions() {
        int parallelism = context.currentParallelism();
        return subscriber.getSubscribedTopicPartitions(pulsarAdmin, rangeGenerator, parallelism);
    }

    /**
     * Check if there's any partition changes within subscribed topic partitions fetched by worker
     * thread, and convert them to splits the assign them to pulsar readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param fetchedPartitions Map from topic name to its description
     * @param throwable Exception in worker thread
     */
    private void checkPartitionChanges(Set<TopicPartition> fetchedPartitions, Throwable throwable) {
        if (throwable != null) {
            throw new FlinkRuntimeException(
                    "Failed to list subscribed topic partitions due to ", throwable);
        }

        // Append the partitions into current assignment state.
        List<TopicPartition> newPartitions =
                splitAssigner.registerTopicPartitions(fetchedPartitions);
        createSubscription(newPartitions);

        // Assign the new readers.
        List<Integer> registeredReaders = new ArrayList<>(context.registeredReaders().keySet());
        assignPendingPartitionSplits(registeredReaders);
    }

    /** Create subscription on topic partition if it doesn't exist. */
    private void createSubscription(List<TopicPartition> newPartitions) {
        for (TopicPartition partition : newPartitions) {
            String topicName = partition.getFullTopicName();
            String subscriptionName = sourceConfiguration.getSubscriptionName();

            List<String> subscriptions =
                    sneakyAdmin(() -> pulsarAdmin.topics().getSubscriptions(topicName));
            if (!subscriptions.contains(subscriptionName)) {
                CursorPosition position =
                        startCursor.position(partition.getTopic(), partition.getPartitionId());
                MessageId initialPosition = queryInitialPosition(topicName, position);

                sneakyAdmin(
                        () ->
                                pulsarAdmin
                                        .topics()
                                        .createSubscription(
                                                topicName, subscriptionName, initialPosition));
            }
        }
    }

    /** Query the available message id from Pulsar. */
    private MessageId queryInitialPosition(String topicName, CursorPosition position) {
        CursorPosition.Type type = position.getType();
        if (type == CursorPosition.Type.TIMESTAMP) {
            return sneakyAdmin(
                    () ->
                            pulsarAdmin
                                    .topics()
                                    .getMessageIdByTimestamp(topicName, position.getTimestamp()));
        } else if (type == CursorPosition.Type.MESSAGE_ID) {
            return position.getMessageId();
        } else {
            throw new UnsupportedOperationException("We don't support this seek type " + type);
        }
    }

    /** Query the unassigned splits and assign them to the available readers. */
    private void assignPendingPartitionSplits(List<Integer> pendingReaders) {
        // Validate the reader.
        pendingReaders.forEach(
                reader -> {
                    if (!context.registeredReaders().containsKey(reader)) {
                        throw new IllegalStateException(
                                "Reader " + reader + " is not registered to source coordinator");
                    }
                });

        // Assign splits to downstream readers.
        splitAssigner.createAssignment(pendingReaders).ifPresent(context::assignSplits);

        // If periodically partition discovery is disabled and the initializing discovery has done,
        // signal NoMoreSplitsEvent to pending readers.
        for (Integer reader : pendingReaders) {
            if (splitAssigner.noMoreSplits(reader)) {
                LOG.debug(
                        "No more PulsarPartitionSplits to assign."
                                + " Sending NoMoreSplitsEvent to reader {} in subscription {}.",
                        reader,
                        sourceConfiguration.getSubscriptionDesc());
                context.signalNoMoreSplits(reader);
            }
        }
    }
}

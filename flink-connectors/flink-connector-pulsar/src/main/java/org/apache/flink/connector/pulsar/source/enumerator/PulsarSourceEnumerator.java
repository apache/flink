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
import org.apache.flink.connector.pulsar.common.config.ConfigurationDataCustomizer;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils.createAdmin;

/** The enumerator class for pulsar source. */
@Internal
public class PulsarSourceEnumerator
        implements SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceEnumerator.class);

    /** The admin interface for pulsar. */
    private final PulsarAdmin pulsarAdmin;

    private final PulsarSubscriber subscriber;
    private final RangeGenerator rangeGenerator;
    private final Configuration configuration;
    private final SourceConfiguration sourceConfiguration;
    private final SplitEnumeratorContext<PulsarPartitionSplit> context;
    private final SplitsAssignmentState assignmentState;

    public PulsarSourceEnumerator(
            PulsarSubscriber subscriber,
            RangeGenerator rangeGenerator,
            Configuration configuration,
            SourceConfiguration sourceConfiguration,
            ConfigurationDataCustomizer<ClientConfigurationData> clientConfigurationCustomizer,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            SplitsAssignmentState assignmentState) {
        this.pulsarAdmin = createAdmin(configuration, clientConfigurationCustomizer);
        this.subscriber = subscriber;
        this.rangeGenerator = rangeGenerator;
        this.configuration = configuration;
        this.sourceConfiguration = sourceConfiguration;
        this.context = context;
        this.assignmentState = assignmentState;
    }

    @Override
    public void start() {
        rangeGenerator.open(configuration, sourceConfiguration);

        // Check the pulsar topic information and convert it into source split.
        if (sourceConfiguration.enablePartitionDiscovery()) {
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
        assignmentState.putSplitsBackToPendingList(splits, subtaskId);

        // If the failed subtask has already restarted, we need to assign pending splits to it
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
        return assignmentState.snapshotState();
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
        return subscriber.getSubscribedTopicPartitions(
                pulsarAdmin, rangeGenerator, context.currentParallelism());
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
        assignmentState.appendTopicPartitions(fetchedPartitions);
        List<Integer> registeredReaders = new ArrayList<>(context.registeredReaders().keySet());

        // Assign the new readers.
        assignPendingPartitionSplits(registeredReaders);
    }

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
        assignmentState.assignSplits(pendingReaders).ifPresent(context::assignSplits);

        // If periodically partition discovery is disabled and the initializing discovery has done,
        // signal NoMoreSplitsEvent to pending readers
        if (assignmentState.noMoreNewPartitionSplits()) {
            LOG.debug(
                    "No more PulsarPartitionSplits to assign."
                            + " Sending NoMoreSplitsEvent to reader {} in subscription {}.",
                    pendingReaders,
                    sourceConfiguration.getSubscriptionDesc());
            pendingReaders.forEach(this.context::signalNoMoreSplits);
        }
    }
}

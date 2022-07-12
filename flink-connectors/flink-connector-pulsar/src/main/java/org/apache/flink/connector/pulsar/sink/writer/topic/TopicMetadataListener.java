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

package org.apache.flink.connector.pulsar.sink.writer.topic;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;

import org.apache.flink.shaded.guava30.com.google.common.base.Objects;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createAdmin;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isPartition;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithoutPartition;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;

/**
 * We need the latest topic metadata for making sure the newly created topic partitions would be
 * used by the Pulsar sink. This routing policy would be different compared with Pulsar Client
 * built-in logic. We use Flink's ProcessingTimer as the executor.
 */
@Internal
public class TopicMetadataListener implements Serializable, Closeable {
    private static final long serialVersionUID = 6186948471557507522L;

    private static final Logger LOG = LoggerFactory.getLogger(TopicMetadataListener.class);

    private final ImmutableList<String> partitionedTopics;
    private final Map<String, Integer> topicMetadata;
    private volatile ImmutableList<String> availableTopics;

    // Dynamic fields.
    private transient PulsarAdmin pulsarAdmin;
    private transient Long topicMetadataRefreshInterval;
    private transient ProcessingTimeService timeService;

    public TopicMetadataListener() {
        this(emptyList());
    }

    public TopicMetadataListener(List<String> topics) {
        List<String> partitions = new ArrayList<>(topics.size());
        Map<String, Integer> metadata = new HashMap<>(topics.size());
        for (String topic : topics) {
            if (isPartition(topic)) {
                partitions.add(topic);
            } else {
                // This would be updated when open writing.
                metadata.put(topic, -1);
            }
        }

        this.partitionedTopics = ImmutableList.copyOf(partitions);
        this.topicMetadata = metadata;
        this.availableTopics = ImmutableList.of();
    }

    /** Register the topic metadata update in process time service. */
    public void open(SinkConfiguration sinkConfiguration, ProcessingTimeService timeService) {
        if (topicMetadata.isEmpty()) {
            LOG.info("No topics have been provided, skip listener initialize.");
            return;
        }

        // Initialize listener properties.
        this.pulsarAdmin = createAdmin(sinkConfiguration);
        this.topicMetadataRefreshInterval = sinkConfiguration.getTopicMetadataRefreshInterval();
        this.timeService = timeService;

        // Initialize the topic metadata. Quit if fail to connect to Pulsar.
        sneakyAdmin(this::updateTopicMetadata);

        // Register time service.
        triggerNextTopicMetadataUpdate(true);
    }

    /**
     * Return all the available topic partitions. We would recalculate the partitions if the topic
     * metadata has been changed. Otherwise, we would return the cached result for better
     * performance.
     */
    public List<String> availableTopics() {
        if (availableTopics.isEmpty()
                && (!partitionedTopics.isEmpty() || !topicMetadata.isEmpty())) {
            List<String> results = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : topicMetadata.entrySet()) {
                int partitionNums = entry.getValue();
                // Get all topics from partitioned and non-partitioned topic names
                if (partitionNums == NON_PARTITIONED) {
                    results.add(topicNameWithoutPartition(entry.getKey()));
                } else {
                    for (int i = 0; i < partitionNums; i++) {
                        results.add(topicNameWithPartition(entry.getKey(), i));
                    }
                }
            }

            results.addAll(partitionedTopics);
            this.availableTopics = ImmutableList.copyOf(results);
        }

        return availableTopics;
    }

    @Override
    public void close() throws IOException {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

    private void triggerNextTopicMetadataUpdate(boolean initial) {
        if (!initial) {
            // We should update the topic metadata, ignore the pulsar admin exception.
            try {
                updateTopicMetadata();
            } catch (PulsarAdminException e) {
                LOG.warn("", e);
            }
        }

        // Register next timer.
        long currentProcessingTime = timeService.getCurrentProcessingTime();
        long triggerTime = currentProcessingTime + topicMetadataRefreshInterval;
        timeService.registerTimer(triggerTime, time -> triggerNextTopicMetadataUpdate(false));
    }

    private void updateTopicMetadata() throws PulsarAdminException {
        boolean shouldUpdate = false;

        for (Map.Entry<String, Integer> entry : topicMetadata.entrySet()) {
            String topic = entry.getKey();
            PartitionedTopicMetadata metadata =
                    pulsarAdmin.topics().getPartitionedTopicMetadata(topic);

            // Update topic metadata if it has been changed.
            if (!Objects.equal(entry.getValue(), metadata.partitions)) {
                entry.setValue(metadata.partitions);
                shouldUpdate = true;
            }
        }

        // Clear available topics if the topic metadata has been changed.
        if (shouldUpdate) {
            this.availableTopics = ImmutableList.of();
        }
    }
}

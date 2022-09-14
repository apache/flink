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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl;

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isPartition;

/** the implements of consuming multiple topics. */
public class TopicListSubscriber extends BasePulsarSubscriber {
    private static final long serialVersionUID = 6473918213832993116L;

    private final List<String> partitions;
    private final List<String> fullTopicNames;

    public TopicListSubscriber(List<String> fullTopicNameOrPartitions) {
        this.partitions = new ArrayList<>();
        this.fullTopicNames = new ArrayList<>();

        for (String fullTopicNameOrPartition : fullTopicNameOrPartitions) {
            if (isPartition(fullTopicNameOrPartition)) {
                this.partitions.add(fullTopicNameOrPartition);
            } else {
                this.fullTopicNames.add(fullTopicNameOrPartition);
            }
        }
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(
            PulsarAdmin pulsarAdmin, RangeGenerator rangeGenerator, int parallelism) {
        Set<TopicPartition> results = new HashSet<>();

        // Query topics from Pulsar.
        for (String topic : fullTopicNames) {
            TopicMetadata metadata = queryTopicMetadata(pulsarAdmin, topic);
            List<TopicRange> ranges = rangeGenerator.range(metadata, parallelism);
            List<TopicPartition> list = toTopicPartitions(metadata, ranges);

            results.addAll(list);
        }

        for (String partition : partitions) {
            TopicName topicName = TopicName.get(partition);
            String name = topicName.getPartitionedTopicName();
            int index = topicName.getPartitionIndex();

            TopicMetadata metadata = queryTopicMetadata(pulsarAdmin, name);
            List<TopicRange> ranges = rangeGenerator.range(metadata, parallelism);

            for (TopicRange range : ranges) {
                results.add(new TopicPartition(name, index, range));
            }
        }

        return results;
    }
}

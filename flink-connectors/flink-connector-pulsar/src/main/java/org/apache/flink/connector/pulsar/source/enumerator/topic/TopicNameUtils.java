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

package org.apache.flink.connector.pulsar.source.enumerator.topic;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.pulsar.common.naming.TopicName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/** util for topic name. */
@Internal
public final class TopicNameUtils {

    private TopicNameUtils() {
        // No public constructor.
    }

    /** Ensure the given topic name should be a topic without partition information. */
    public static String topicName(String topic) {
        return TopicName.get(topic).getPartitionedTopicName();
    }

    /** Create a topic name with partition information. */
    public static String topicNameWithPartition(String topic, int partitionId) {
        checkArgument(partitionId >= 0, "Illegal partition id %s", partitionId);
        return TopicName.get(topic).getPartition(partitionId).toString();
    }

    /** Get a non-partitioned topic name that does not belong to any partitioned topic. */
    public static String topicNameWithoutPartition(String topic) {
        return TopicName.get(topic).toString();
    }

    public static boolean isPartition(String topic) {
        return TopicName.get(topic).isPartitioned();
    }

    /** Merge the same topics into one topic. */
    public static List<String> distinctTopics(List<String> topics) {
        Set<String> fullTopics = new HashSet<>();
        Map<String, List<Integer>> partitionedTopics = new HashMap<>();

        for (String topic : topics) {
            TopicName topicName = TopicName.get(topic);
            String partitionedTopicName = topicName.getPartitionedTopicName();

            if (!topicName.isPartitioned()) {
                fullTopics.add(partitionedTopicName);
                partitionedTopics.remove(partitionedTopicName);
            } else if (!fullTopics.contains(partitionedTopicName)) {
                List<Integer> partitionIds =
                        partitionedTopics.computeIfAbsent(
                                partitionedTopicName, k -> new ArrayList<>());
                partitionIds.add(topicName.getPartitionIndex());
            }
        }

        ImmutableList.Builder<String> builder = ImmutableList.<String>builder().addAll(fullTopics);

        for (Map.Entry<String, List<Integer>> topicSet : partitionedTopics.entrySet()) {
            String topicName = topicSet.getKey();
            for (Integer partitionId : topicSet.getValue()) {
                builder.add(topicNameWithPartition(topicName, partitionId));
            }
        }

        return builder.build();
    }
}

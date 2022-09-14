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
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.pulsar.common.naming.TopicName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicDomain.persistent;

/** util for topic name. */
@Internal
public final class TopicNameUtils {

    private static final Pattern HEARTBEAT_NAMESPACE_PATTERN =
            Pattern.compile("pulsar/[^/]+/([^:]+:\\d+)");
    private static final Pattern HEARTBEAT_NAMESPACE_PATTERN_V2 =
            Pattern.compile("pulsar/([^:]+:\\d+)");
    private static final Pattern SLA_NAMESPACE_PATTERN =
            Pattern.compile("sla-monitor" + "/[^/]+/([^:]+:\\d+)");
    private static final Set<String> EVENTS_TOPIC_NAMES =
            ImmutableSet.of("__change_events", "__transaction_buffer_snapshot");
    private static final String TRANSACTION_COORDINATOR_ASSIGN_PREFIX =
            TopicName.get(persistent.value(), SYSTEM_NAMESPACE, "transaction_coordinator_assign")
                    .toString();
    private static final String TRANSACTION_COORDINATOR_LOG_PREFIX =
            TopicName.get(persistent.value(), SYSTEM_NAMESPACE, "__transaction_log_").toString();
    private static final String PENDING_ACK_STORE_SUFFIX = "__transaction_pending_ack";
    private static final String PENDING_ACK_STORE_CURSOR_SUFFIX = "__pending_ack_state";

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

    /**
     * This method is refactored from {@code BrokerService} in pulsar-broker which is not available
     * in the Pulsar client. We have to put it here and self maintained. Since these topic names
     * would never be changed for backward compatible, we only need to add new topic names after
     * version bump.
     *
     * @see <a
     *     href="https://github.com/apache/pulsar/blob/7075a5ce0d4a70f52625ac8c3d0c48894442b72a/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/BrokerService.java#L3024">BrokerService#isSystemTopic</a>
     */
    public static boolean isInternal(String topic) {
        // A topic name instance without partition information.
        String topicName = topicName(topic);
        TopicName topicInstance = TopicName.get(topicName);
        String localName = topicInstance.getLocalName();
        String namespace = topicInstance.getNamespace();

        return namespace.equals(SYSTEM_NAMESPACE.toString())
                || SLA_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN_V2.matcher(namespace).matches()
                || EVENTS_TOPIC_NAMES.contains(localName)
                || topicName.startsWith(TRANSACTION_COORDINATOR_ASSIGN_PREFIX)
                || topicName.startsWith(TRANSACTION_COORDINATOR_LOG_PREFIX)
                || localName.endsWith(PENDING_ACK_STORE_SUFFIX)
                || localName.endsWith(PENDING_ACK_STORE_CURSOR_SUFFIX);
    }
}

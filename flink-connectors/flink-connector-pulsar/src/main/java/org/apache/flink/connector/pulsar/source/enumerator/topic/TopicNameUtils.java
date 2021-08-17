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

import org.apache.pulsar.common.naming.TopicName;

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
}

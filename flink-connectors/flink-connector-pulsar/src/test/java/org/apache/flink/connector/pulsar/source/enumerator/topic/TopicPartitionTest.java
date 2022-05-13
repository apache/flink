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

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link TopicPartition}. */
class TopicPartitionTest {

    @Test
    void topicNameForPartitionedAndNonPartitionedTopic() {
        // For partitioned topic
        TopicPartition partition = new TopicPartition("test-name", 12, createFullRange());
        assertEquals(
                partition.getFullTopicName(), "persistent://public/default/test-name-partition-12");

        // For non-partitioned topic
        TopicPartition partition1 = new TopicPartition("test-topic", -1, createFullRange());
        assertEquals(partition1.getFullTopicName(), "persistent://public/default/test-topic");
    }
}

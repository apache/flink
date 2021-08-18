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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link TopicNameUtils}. */
class TopicNameUtilsTest {

    private static final String fullTopicName = "persistent://tenant/cluster/namespace/topic";
    private static final String topicNameWithLocal =
            "persistent://tenant/cluster/namespace/ns-abc/table/1";
    private static final String shortTopicName = "short-topic";
    private static final String topicNameWithoutCluster = "persistent://tenant/namespace/topic";

    @Test
    void topicNameWouldReturnACleanTopicNameWithTenant() {
        String name1 = TopicNameUtils.topicName(fullTopicName + "-partition-1");
        assertEquals(name1, fullTopicName);

        String name2 = TopicNameUtils.topicName(topicNameWithLocal);
        assertEquals(name2, topicNameWithLocal);

        String name3 = TopicNameUtils.topicName(shortTopicName + "-partition-1");
        assertEquals(name3, "persistent://public/default/short-topic");

        String name4 = TopicNameUtils.topicName(shortTopicName);
        assertEquals(name4, "persistent://public/default/short-topic");

        String name5 = TopicNameUtils.topicName(topicNameWithoutCluster + "-partition-1");
        assertEquals(name5, topicNameWithoutCluster);
    }

    @Test
    void topicNameWithPartitionInfo() {
        assertThrows(
                IllegalArgumentException.class,
                () -> TopicNameUtils.topicNameWithPartition(shortTopicName, -3));

        String name1 = TopicNameUtils.topicNameWithPartition(fullTopicName, 4);
        assertEquals(name1, fullTopicName + "-partition-4");

        String name2 = TopicNameUtils.topicNameWithPartition(topicNameWithLocal, 3);
        assertEquals(name2, topicNameWithLocal + "-partition-3");

        String name3 = TopicNameUtils.topicNameWithPartition(shortTopicName, 5);
        assertEquals(name3, "persistent://public/default/short-topic-partition-5");

        String name4 = TopicNameUtils.topicNameWithPartition(topicNameWithoutCluster, 8);
        assertEquals(name4, topicNameWithoutCluster + "-partition-8");
    }
}

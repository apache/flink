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

package org.apache.flink.connector.kafka.source.enumerator.subscriber;

import org.apache.flink.connector.kafka.source.KafkaSourceTestEnv;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/** Unit tests for {@link KafkaSubscriber}. */
public class KafkaSubscriberTest {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "pattern-topic";
    private static final TopicPartition assignedPartition1 = new TopicPartition(TOPIC1, 2);
    private static final TopicPartition assignedPartition2 = new TopicPartition(TOPIC2, 2);
    private static final TopicPartition removedPartition = new TopicPartition("removed", 0);
    private static final Set<TopicPartition> currentAssignment =
            new HashSet<>(Arrays.asList(assignedPartition1, assignedPartition2, removedPartition));
    private static AdminClient adminClient;

    @BeforeClass
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.createTestTopic(TOPIC1);
        KafkaSourceTestEnv.createTestTopic(TOPIC2);
        adminClient = KafkaSourceTestEnv.getAdminClient();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        adminClient.close();
        KafkaSourceTestEnv.tearDown();
    }

    @Test
    public void testTopicListSubscriber() {
        List<String> topics = Arrays.asList(TOPIC1, TOPIC2);
        KafkaSubscriber subscriber =
                KafkaSubscriber.getTopicListSubscriber(Arrays.asList(TOPIC1, TOPIC2));
        KafkaSubscriber.PartitionChange change =
                subscriber.getPartitionChanges(adminClient, currentAssignment);
        Set<TopicPartition> expectedNewPartitions =
                new HashSet<>(KafkaSourceTestEnv.getPartitionsForTopics(topics));
        expectedNewPartitions.remove(assignedPartition1);
        expectedNewPartitions.remove(assignedPartition2);
        assertEquals(expectedNewPartitions, change.getNewPartitions());
        assertEquals(Collections.singleton(removedPartition), change.getRemovedPartitions());
    }

    @Test
    public void testTopicPatternSubscriber() {
        KafkaSubscriber subscriber =
                KafkaSubscriber.getTopicPatternSubscriber(Pattern.compile("pattern.*"));
        KafkaSubscriber.PartitionChange change =
                subscriber.getPartitionChanges(adminClient, currentAssignment);

        Set<TopicPartition> expectedNewPartitions = new HashSet<>();
        for (int i = 0; i < KafkaSourceTestEnv.NUM_PARTITIONS; i++) {
            if (i != assignedPartition2.partition()) {
                expectedNewPartitions.add(new TopicPartition(TOPIC2, i));
            }
        }
        Set<TopicPartition> expectedRemovedPartitions =
                new HashSet<>(Arrays.asList(assignedPartition1, removedPartition));

        assertEquals(expectedNewPartitions, change.getNewPartitions());
        assertEquals(expectedRemovedPartitions, change.getRemovedPartitions());
    }

    @Test
    public void testPartitionSetSubscriber() {
        List<String> topics = Arrays.asList(TOPIC1, TOPIC2);
        Set<TopicPartition> partitions =
                new HashSet<>(KafkaSourceTestEnv.getPartitionsForTopics(topics));
        partitions.remove(new TopicPartition(TOPIC1, 1));

        KafkaSubscriber subscriber = KafkaSubscriber.getPartitionSetSubscriber(partitions);
        KafkaSubscriber.PartitionChange change =
                subscriber.getPartitionChanges(adminClient, currentAssignment);

        Set<TopicPartition> expectedNewPartitions = new HashSet<>(partitions);
        expectedNewPartitions.remove(assignedPartition1);
        expectedNewPartitions.remove(assignedPartition2);
        assertEquals(expectedNewPartitions, change.getNewPartitions());
        assertEquals(Collections.singleton(removedPartition), change.getRemovedPartitions());
    }
}

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

import org.apache.flink.connector.kafka.source.testutils.KafkaSourceTestEnv;
import org.apache.flink.util.ExceptionUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link KafkaSubscriber}. */
public class KafkaSubscriberTest {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "pattern-topic";
    private static final TopicPartition NON_EXISTING_TOPIC = new TopicPartition("removed", 0);
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
        final Set<TopicPartition> subscribedPartitions =
                subscriber.getSubscribedTopicPartitions(adminClient);

        final Set<TopicPartition> expectedSubscribedPartitions =
                new HashSet<>(KafkaSourceTestEnv.getPartitionsForTopics(topics));

        assertEquals(expectedSubscribedPartitions, subscribedPartitions);
    }

    @Test
    public void testNonExistingTopic() {
        final KafkaSubscriber subscriber =
                KafkaSubscriber.getTopicListSubscriber(
                        Collections.singletonList(NON_EXISTING_TOPIC.topic()));

        Throwable t =
                assertThrows(
                        RuntimeException.class,
                        () -> subscriber.getSubscribedTopicPartitions(adminClient));

        assertTrue(
                "Exception should be caused by UnknownTopicOrPartitionException",
                ExceptionUtils.findThrowable(t, UnknownTopicOrPartitionException.class)
                        .isPresent());
    }

    @Test
    public void testTopicPatternSubscriber() {
        KafkaSubscriber subscriber =
                KafkaSubscriber.getTopicPatternSubscriber(Pattern.compile("pattern.*"));
        final Set<TopicPartition> subscribedPartitions =
                subscriber.getSubscribedTopicPartitions(adminClient);

        final Set<TopicPartition> expectedSubscribedPartitions =
                new HashSet<>(
                        KafkaSourceTestEnv.getPartitionsForTopics(Collections.singleton(TOPIC2)));

        assertEquals(expectedSubscribedPartitions, subscribedPartitions);
    }

    @Test
    public void testPartitionSetSubscriber() {
        List<String> topics = Arrays.asList(TOPIC1, TOPIC2);
        Set<TopicPartition> partitions =
                new HashSet<>(KafkaSourceTestEnv.getPartitionsForTopics(topics));
        partitions.remove(new TopicPartition(TOPIC1, 1));

        KafkaSubscriber subscriber = KafkaSubscriber.getPartitionSetSubscriber(partitions);

        final Set<TopicPartition> subscribedPartitions =
                subscriber.getSubscribedTopicPartitions(adminClient);

        assertEquals(partitions, subscribedPartitions);
    }

    @Test
    public void testNonExistingPartition() {
        TopicPartition nonExistingPartition = new TopicPartition(TOPIC1, Integer.MAX_VALUE);
        final KafkaSubscriber subscriber =
                KafkaSubscriber.getPartitionSetSubscriber(
                        Collections.singleton(nonExistingPartition));

        Throwable t =
                assertThrows(
                        RuntimeException.class,
                        () -> subscriber.getSubscribedTopicPartitions(adminClient));

        assertEquals(
                String.format(
                        "Partition '%s' does not exist on Kafka brokers", nonExistingPartition),
                t.getMessage());
    }
}

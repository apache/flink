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

package org.apache.flink.connector.pulsar.sink.writer.router;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link RoundRobinTopicRouter}. */
class RoundRobinTopicRouterTest {

    @Test
    void routeMessageByEmptyTopics() {
        SinkConfiguration configuration = sinkConfiguration(10);
        RoundRobinTopicRouter<String> router = new RoundRobinTopicRouter<>(configuration);

        String message = randomAlphabetic(10);
        List<String> partitions = emptyList();
        PulsarSinkContext context = mock(PulsarSinkContext.class);

        assertThrows(
                IllegalArgumentException.class,
                () -> router.route(message, null, partitions, context));
    }

    @Test
    void routeMessagesInRoundRobin() {
        int batchSize = ThreadLocalRandom.current().nextInt(20) + 1;
        SinkConfiguration configuration = sinkConfiguration(batchSize);
        RoundRobinTopicRouter<String> router = new RoundRobinTopicRouter<>(configuration);

        List<String> topics = ImmutableList.of("topic1", "topic2");
        PulsarSinkContext context = mock(PulsarSinkContext.class);

        for (int i = 0; i < batchSize; i++) {
            String message = randomAlphabetic(10);
            String topic = router.route(message, null, topics, context);
            assertEquals(topic, topics.get(0));
        }

        for (int i = 0; i < batchSize; i++) {
            String message = randomAlphabetic(10);
            String topic = router.route(message, null, topics, context);
            assertEquals(topic, topics.get(1));
        }

        String message = randomAlphabetic(10);
        String topic = router.route(message, null, topics, context);
        assertEquals(topic, topics.get(0));
    }

    private SinkConfiguration sinkConfiguration(int switchSize) {
        Configuration configuration = new Configuration();
        configuration.set(PULSAR_BATCHING_MAX_MESSAGES, switchSize);

        return new SinkConfiguration(configuration);
    }
}

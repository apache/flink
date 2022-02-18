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

import org.apache.pulsar.client.impl.Hash;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MESSAGE_KEY_HASH;
import static org.apache.pulsar.client.util.MathUtils.signSafeMod;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link KeyHashTopicRouter}. */
class KeyHashTopicRouterTest {

    @ParameterizedTest
    @EnumSource(MessageKeyHash.class)
    void routeWithEmptyPartition(MessageKeyHash keyHash) {
        SinkConfiguration configuration = sinkConfiguration(keyHash);
        KeyHashTopicRouter<String> router = new KeyHashTopicRouter<>(configuration);

        String message = randomAlphanumeric(10);
        String key = randomAlphanumeric(10);
        List<String> emptyTopics = emptyList();
        PulsarSinkContext sinkContext = mock(PulsarSinkContext.class);

        assertThrows(
                IllegalArgumentException.class,
                () -> router.route(message, key, emptyTopics, sinkContext));
    }

    @ParameterizedTest
    @EnumSource(MessageKeyHash.class)
    void routeOnlyOnePartition(MessageKeyHash keyHash) {
        SinkConfiguration configuration = sinkConfiguration(keyHash);
        List<String> topics = singletonList(randomAlphanumeric(10));

        KeyHashTopicRouter<String> router1 = new KeyHashTopicRouter<>(configuration);
        String topic1 =
                router1.route(
                        randomAlphanumeric(10),
                        randomAlphanumeric(10),
                        topics,
                        mock(PulsarSinkContext.class));
        assertEquals(topic1, topics.get(0));

        KeyHashTopicRouter<String> router2 = new KeyHashTopicRouter<>(configuration);
        String topic2 =
                router2.route(randomAlphanumeric(10), null, topics, mock(PulsarSinkContext.class));
        assertEquals(topic2, topics.get(0));
    }

    @ParameterizedTest
    @EnumSource(MessageKeyHash.class)
    void routeMessageByMessageKey(MessageKeyHash keyHash) {
        SinkConfiguration configuration = sinkConfiguration(keyHash);
        String messageKey = randomAlphanumeric(10);
        KeyHashTopicRouter<String> router = new KeyHashTopicRouter<>(configuration);

        List<String> topics =
                Stream.generate(() -> randomAlphanumeric(10))
                        .distinct()
                        .limit(10)
                        .collect(toList());

        Hash hash = keyHash.getHash();
        int index = signSafeMod(hash.makeHash(messageKey), topics.size());
        String desiredTopic = topics.get(index);
        String message = randomAlphanumeric(10);

        String topic = router.route(message, messageKey, topics, mock(PulsarSinkContext.class));

        assertEquals(topic, desiredTopic);
    }

    private SinkConfiguration sinkConfiguration(MessageKeyHash hash) {
        Configuration configuration = new Configuration();
        configuration.set(PULSAR_MESSAGE_KEY_HASH, hash);

        return new SinkConfiguration(configuration);
    }
}

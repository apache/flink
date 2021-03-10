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

package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test the source split serializer. */
public class RabbitMQSourceSplitSerializerTest {

    private RabbitMQSourceSplit getSourceSplit() {

        String queueName = "exampleQueueName";
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            ids.add(Integer.toString(i));
        }
        Set<String> correlationIds = new HashSet<>(ids);
        final RabbitMQConnectionConfig connectionConfig =
                new RabbitMQConnectionConfig.Builder()
                        .setHost("Host")
                        .setVirtualHost("/")
                        .setUserName("Username")
                        .setPassword("Password")
                        .setPort(3000)
                        .build();
        return new RabbitMQSourceSplit(connectionConfig, queueName, correlationIds);
    }

    @Test
    public void testSplitSerializer() throws IOException {
        RabbitMQSourceSplit split = getSourceSplit();
        RabbitMQSourceSplitSerializer serializer = new RabbitMQSourceSplitSerializer();

        byte[] serializedSplit = serializer.serialize(split);
        RabbitMQSourceSplit deserializedSplit = serializer.deserialize(1, serializedSplit);

        assertNotNull(deserializedSplit);
        assertEquals(split.splitId(), deserializedSplit.splitId());
        assertEquals(split.getCorrelationIds(), deserializedSplit.getCorrelationIds());
        assertEquals(split.getQueueName(), deserializedSplit.getQueueName());
        assertEquals(
                split.getConnectionConfig().getHost(),
                deserializedSplit.getConnectionConfig().getHost());
    }
}

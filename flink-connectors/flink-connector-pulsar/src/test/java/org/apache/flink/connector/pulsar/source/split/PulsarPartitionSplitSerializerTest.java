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

package org.apache.flink.connector.pulsar.source.split;

import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import org.junit.jupiter.api.Test;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitSerializer.INSTANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/** Unit tests for {@link PulsarPartitionSplitSerializer}. */
class PulsarPartitionSplitSerializerTest {

    @Test
    void serializeAndDeserializePulsarPartitionSplit() throws Exception {
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(
                        new TopicPartition(randomAlphabetic(10), 10, createFullRange()),
                        StopCursor.defaultStopCursor());

        byte[] bytes = INSTANCE.serialize(split);
        PulsarPartitionSplit split1 = INSTANCE.deserialize(INSTANCE.getVersion(), bytes);

        assertEquals(split, split1);
        assertNotSame(split, split1);
    }
}

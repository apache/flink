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

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.MAX_RANGE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link TopicRange}. */
class TopicRangeTest {

    private final Random random = new Random(System.currentTimeMillis());

    @RepeatedTest(10)
    @SuppressWarnings("java:S5778")
    void rangeCreationHaveALimitedScope() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new TopicRange(-1, random.nextInt(MAX_RANGE)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TopicRange(1, MAX_RANGE + random.nextInt(10000)));

        assertDoesNotThrow(() -> new TopicRange(1, random.nextInt(MAX_RANGE)));
    }

    @Test
    void topicRangeIsSerializable() throws Exception {
        TopicRange range = new TopicRange(10, random.nextInt(MAX_RANGE));
        TopicRange cloneRange = InstantiationUtil.clone(range);

        assertEquals(range, cloneRange);
    }
}

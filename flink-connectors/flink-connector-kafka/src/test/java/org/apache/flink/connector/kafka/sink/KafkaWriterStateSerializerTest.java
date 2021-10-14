/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for serializing and deserialzing {@link KafkaWriterState} with {@link
 * KafkaWriterStateSerializer}.
 */
public class KafkaWriterStateSerializerTest extends TestLogger {

    private static final KafkaWriterStateSerializer SERIALIZER = new KafkaWriterStateSerializer();

    @Test
    public void testStateSerDe() throws IOException {
        final KafkaWriterState state = new KafkaWriterState("idPrefix");
        final byte[] serialized = SERIALIZER.serialize(state);
        assertEquals(state, SERIALIZER.deserialize(1, serialized));
    }
}

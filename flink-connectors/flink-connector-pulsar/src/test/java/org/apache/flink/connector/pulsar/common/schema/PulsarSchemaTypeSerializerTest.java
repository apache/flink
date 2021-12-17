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

package org.apache.flink.connector.pulsar.common.schema;

import org.apache.flink.api.common.typeutils.ComparatorTestBase.TestInputView;
import org.apache.flink.api.common.typeutils.ComparatorTestBase.TestOutputView;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.connector.pulsar.SampleMessage.TestEnum;
import org.apache.flink.connector.pulsar.SampleMessage.TestMessage;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.nCopies;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Unit tests for {@link PulsarSchemaTypeSerializer}. */
class PulsarSchemaTypeSerializerTest {

    private final PulsarSchemaTypeSerializer<TestMessage> serializer =
            new PulsarSchemaTypeSerializer<>(
                    new PulsarSchema<>(PROTOBUF_NATIVE(TestMessage.class), TestMessage.class));

    private final TestMessage message =
            TestMessage.newBuilder()
                    .setStringField(randomAlphabetic(10))
                    .setDoubleField(ThreadLocalRandom.current().nextDouble())
                    .setIntField(ThreadLocalRandom.current().nextInt())
                    .setTestEnum(TestEnum.SHARED)
                    .addAllRepeatedField(nCopies(10, randomAlphabetic(13)))
                    .build();

    @Test
    void createDeserializeRecordInstance() {
        // Protobuf class creation
        assertNotNull(serializer.createInstance());
    }

    @Test
    void serializeAndDeserializeInstance() throws Exception {
        TestOutputView output = new TestOutputView();
        serializer.serialize(message, output);

        TestInputView input = output.getInputView();
        TestMessage message1 = serializer.deserialize(input);

        assertEquals(message, message1);
    }

    @Test
    void copyValueAmongDataView() throws Exception {
        TestOutputView output1 = new TestOutputView();
        serializer.serialize(message, output1);

        // Copy bytes among view.
        TestInputView input1 = output1.getInputView();
        TestOutputView output2 = new TestOutputView();
        serializer.copy(input1, output2);

        TestInputView input2 = output2.getInputView();
        TestMessage message1 = serializer.deserialize(input2);
        assertEquals(message, message1);
    }

    @Test
    void snapshotSerializerAndRecreateIt() throws Exception {
        TypeSerializerSnapshot<TestMessage> snapshot = serializer.snapshotConfiguration();

        // Snapshot
        TestOutputView output = new TestOutputView();
        snapshot.writeSnapshot(output);

        // Restore from snapshot
        snapshot.readSnapshot(
                snapshot.getCurrentVersion(),
                output.getInputView(),
                PulsarSchemaTypeSerializerTest.class.getClassLoader());

        TypeSerializer<TestMessage> serializer1 = snapshot.restoreSerializer();
        assertEquals(serializer, serializer1);
    }
}

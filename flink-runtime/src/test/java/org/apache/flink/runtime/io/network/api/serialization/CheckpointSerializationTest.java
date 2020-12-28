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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests the {@link EventSerializer} functionality for serializing {@link CheckpointBarrier
 * checkpoint barriers}.
 */
public class CheckpointSerializationTest {

    private static final byte[] STORAGE_LOCATION_REF =
            new byte[] {15, 52, 52, 11, 0, 0, 0, 0, -1, -23, -19, 35};

    @Test
    public void testSuspendingCheckpointBarrierSerialization() throws Exception {
        CheckpointOptions suspendSavepointToSerialize =
                new CheckpointOptions(
                        CheckpointType.SYNC_SAVEPOINT,
                        new CheckpointStorageLocationReference(STORAGE_LOCATION_REF));
        testCheckpointBarrierSerialization(suspendSavepointToSerialize);
    }

    @Test
    public void testSavepointBarrierSerialization() throws Exception {
        CheckpointOptions savepointToSerialize =
                new CheckpointOptions(
                        CheckpointType.SAVEPOINT,
                        new CheckpointStorageLocationReference(STORAGE_LOCATION_REF));
        testCheckpointBarrierSerialization(savepointToSerialize);
    }

    @Test
    public void testCheckpointBarrierSerialization() throws Exception {
        CheckpointOptions checkpointToSerialize =
                new CheckpointOptions(
                        CheckpointType.CHECKPOINT,
                        new CheckpointStorageLocationReference(STORAGE_LOCATION_REF));
        testCheckpointBarrierSerialization(checkpointToSerialize);
    }

    @Test
    public void testCheckpointWithDefaultLocationSerialization() throws Exception {
        CheckpointOptions checkpointToSerialize =
                CheckpointOptions.forCheckpointWithDefaultLocation();
        testCheckpointBarrierSerialization(checkpointToSerialize);
    }

    private void testCheckpointBarrierSerialization(CheckpointOptions options) throws IOException {
        final long checkpointId = Integer.MAX_VALUE + 123123L;
        final long timestamp = Integer.MAX_VALUE + 1228L;

        final CheckpointBarrier barrierBeforeSerialization =
                new CheckpointBarrier(checkpointId, timestamp, options);
        final CheckpointBarrier barrierAfterDeserialization =
                serializeAndDeserializeCheckpointBarrier(barrierBeforeSerialization);

        assertEquals(barrierBeforeSerialization, barrierAfterDeserialization);
    }

    private CheckpointBarrier serializeAndDeserializeCheckpointBarrier(
            final CheckpointBarrier barrierUnderTest) throws IOException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final ByteBuffer serialized = EventSerializer.toSerializedEvent(barrierUnderTest);
        final CheckpointBarrier deserialized =
                (CheckpointBarrier) EventSerializer.fromSerializedEvent(serialized, cl);
        assertFalse(serialized.hasRemaining());
        return deserialized;
    }
}

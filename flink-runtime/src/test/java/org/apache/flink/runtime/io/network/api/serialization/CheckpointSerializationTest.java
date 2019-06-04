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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests the {@link EventSerializer} functionality for serializing {@link CheckpointBarrier checkpoint barriers}.
 */
@RunWith(Parameterized.class)
public class CheckpointSerializationTest {

	private static final byte[] STORAGE_LOCATION_REF = new byte[] { 15, 52, 52, 11, 0, 0, 0, 0, -1, -23, -19, 35 };

	@Parameterized.Parameters(name = "checkpointOptions = {0}")
	public static Collection<CheckpointOptions> parameters () {
		return Arrays.asList(
			new CheckpointOptions(CheckpointType.SYNC_CHECKPOINT, new CheckpointStorageLocationReference(STORAGE_LOCATION_REF)),
			new CheckpointOptions(CheckpointType.SYNC_SAVEPOINT, new CheckpointStorageLocationReference(STORAGE_LOCATION_REF)),
			new CheckpointOptions(CheckpointType.CHECKPOINT, new CheckpointStorageLocationReference(STORAGE_LOCATION_REF)),
			new CheckpointOptions(CheckpointType.SAVEPOINT, new CheckpointStorageLocationReference(STORAGE_LOCATION_REF)),
			CheckpointOptions.forCheckpointWithDefaultLocation());
	}

	@Parameterized.Parameter
	public CheckpointOptions checkpointOptions;

	@Test
	public void testSuspendingCheckpointBarrierSerialization() throws Exception {
		testCheckpointBarrierSerialization(checkpointOptions);
	}

	private void testCheckpointBarrierSerialization(CheckpointOptions options) throws IOException {
		final long checkpointId = Integer.MAX_VALUE + 123123L;
		final long timestamp = Integer.MAX_VALUE + 1228L;

		final CheckpointBarrier barrierBeforeSerialization = new CheckpointBarrier(checkpointId, timestamp, options);
		final CheckpointBarrier barrierAfterDeserialization = serializeAndDeserializeCheckpointBarrier(barrierBeforeSerialization);

		assertEquals(barrierBeforeSerialization, barrierAfterDeserialization);
	}

	private CheckpointBarrier serializeAndDeserializeCheckpointBarrier(final CheckpointBarrier barrierUnderTest) throws IOException {
		final ClassLoader cl = Thread.currentThread().getContextClassLoader();
		final ByteBuffer serialized = EventSerializer.toSerializedEvent(barrierUnderTest);
		final CheckpointBarrier deserialized = (CheckpointBarrier) EventSerializer.fromSerializedEvent(serialized, cl);
		assertFalse(serialized.hasRemaining());
		return deserialized;
	}
}

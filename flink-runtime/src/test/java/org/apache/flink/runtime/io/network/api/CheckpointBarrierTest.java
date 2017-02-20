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

package org.apache.flink.runtime.io.network.api;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.junit.Test;

public class CheckpointBarrierTest {

	/**
	 * Test serialization of the checkpoint barrier.
	 */
	@Test
	public void testSerialization() throws Exception {
		long id = Integer.MAX_VALUE + 123123L;
		long timestamp = Integer.MAX_VALUE + 1228L;

		CheckpointOptions checkpoint = CheckpointOptions.forFullCheckpoint();
		testSerialization(id, timestamp, checkpoint);

		CheckpointOptions savepoint = CheckpointOptions.forSavepoint("1289031838919123");
		testSerialization(id, timestamp, savepoint);
	}

	private void testSerialization(long id, long timestamp, CheckpointOptions options) throws IOException {
		CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp, options);

		DataOutputSerializer out = new DataOutputSerializer(1024);
		barrier.write(out);

		DataInputDeserializer in = new DataInputDeserializer(out.wrapAsByteBuffer());
		CheckpointBarrier deserialized = new CheckpointBarrier();
		deserialized.read(in);

		assertEquals(id, deserialized.getId());
		assertEquals(timestamp, deserialized.getTimestamp());
		assertEquals(options.getCheckpointType(), deserialized.getCheckpointOptions().getCheckpointType());
		assertEquals(options.getTargetLocation(), deserialized.getCheckpointOptions().getTargetLocation());
	}
}

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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.io.network.util.TestInfiniteBufferProvider;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ResultPartitionWriterTest {

	/**
	 * Tests that the result partition writer calls the correct partition
	 * methods and sets the correct flags. This mostly important for
	 * indicating whether we want to be capacity constrained or not.
	 */
	@Test
	public void testCorrectCallForwarding() throws Exception {
		ResultPartitionID partitionId = new ResultPartitionID();
		BufferProvider bufferProvider = new TestInfiniteBufferProvider();
		int numSubPartitions = 123;

		ResultPartition partition = mock(ResultPartition.class);
		when(partition.getPartitionId()).thenReturn(partitionId);
		when(partition.getBufferProvider()).thenReturn(bufferProvider);
		when(partition.getNumberOfSubpartitions()).thenReturn(numSubPartitions);

		ResultPartitionWriter writer = new ResultPartitionWriter(partition);

		// - Properties -
		assertEquals(partitionId, writer.getPartitionId());
		assertEquals(bufferProvider, writer.getBufferProvider());
		assertEquals(numSubPartitions, writer.getNumberOfOutputChannels());

		Buffer buffer = TestBufferFactory.getMockBuffer();

		// - Buffer write calls -

		// Back pressure flag
		writer.writeBuffer(buffer, 0, false);
		verify(partition, times(1)).add(eq(buffer), eq(0), eq(false));

		writer.writeBuffer(buffer, 1, true);
		verify(partition, times(1)).add(eq(buffer), eq(1), eq(true));

		// Capacity constraint variants
		writer.tryWriteBuffer(buffer, 2);
		verify(partition, times(1)).addBufferIfCapacityAvailable(eq(buffer), eq(2));

		// - Event calls -

		writer.writeEvent(new CheckpointBarrier(123, 220), 3);
		verify(partition, times(1)).add(Matchers.argThat(new ArgumentMatcher<Buffer>() {
			@Override
			public boolean matches(Object arg) {
				Buffer buffer = (Buffer) arg;
				return !buffer.isBuffer();
			}
		}), eq(3), eq(false));

		// Reset and check all add calls
		Mockito.reset(partition);
		when(partition.getNumberOfSubpartitions()).thenReturn(numSubPartitions);

		writer.writeEventToAllChannels(new CheckpointBarrier(221, 1123));

		for (int i = 0; i < numSubPartitions; i++) {
			verify(partition, times(1)).add(any(Buffer.class), eq(i), eq(false));
		}
	}
}

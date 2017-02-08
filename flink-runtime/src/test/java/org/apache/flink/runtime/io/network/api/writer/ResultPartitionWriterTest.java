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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@PrepareForTest({ResultPartitionWriter.class})
@RunWith(PowerMockRunner.class)
public class ResultPartitionWriterTest {

	// ---------------------------------------------------------------------------------------------
	// Resource release tests
	// ---------------------------------------------------------------------------------------------

	/**
	 * Tests that event buffers are properly recycled when broadcasting events
	 * to multiple channels.
	 *
	 * @throws Exception
	 */
	@Test
	public void testWriteBufferToAllChannelsReferenceCounting() throws Exception {
		Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);

		ResultPartition partition = new ResultPartition(
			"TestTask",
			mock(TaskActions.class),
			new JobID(),
			new ResultPartitionID(),
			ResultPartitionType.PIPELINED,
			2,
			2,
			mock(ResultPartitionManager.class),
			mock(ResultPartitionConsumableNotifier.class),
			mock(IOManager.class),
			false);
		ResultPartitionWriter partitionWriter =
			new ResultPartitionWriter(
				partition);

		partitionWriter.writeBufferToAllChannels(buffer);

		// Verify added to all queues, i.e. two buffers in total
		assertEquals(2, partition.getTotalNumberOfBuffers());
		// release the buffers in the partition
		partition.release();

		assertTrue(buffer.isRecycled());
	}
}

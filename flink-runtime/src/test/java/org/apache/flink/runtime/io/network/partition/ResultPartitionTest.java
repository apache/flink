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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link ResultPartition}.
 */
public class ResultPartitionTest {

	/** Asynchronous I/O manager. */
	private static final IOManager ioManager = new IOManagerAsync();

	@AfterClass
	public static void shutdown() {
		ioManager.shutdown();
	}

	/**
	 * Tests the schedule or update consumers message sending behaviour depending on the relevant flags.
	 */
	@Test
	public void testSendScheduleOrUpdateConsumersMessage() throws Exception {
		{
			// Pipelined, send message => notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.PIPELINED, true);
			partition.writeBuffer(TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE), 0);
			verify(notifier, times(1))
				.notifyPartitionConsumable(
					eq(partition.getJobId()),
					eq(partition.getPartitionId()),
					any(TaskActions.class));
		}

		{
			// Pipelined, don't send message => don't notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.PIPELINED, false);
			partition.writeBuffer(TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE), 0);
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class), any(TaskActions.class));
		}

		{
			// Blocking, send message => don't notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.BLOCKING, true);
			partition.writeBuffer(TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE), 0);
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class), any(TaskActions.class));
		}

		{
			// Blocking, don't send message => don't notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.BLOCKING, false);
			partition.writeBuffer(TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE), 0);
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class), any(TaskActions.class));
		}
	}

	@Test
	public void testAddOnFinishedPipelinedPartition() throws Exception {
		testAddOnFinishedPartition(ResultPartitionType.PIPELINED);
	}

	@Test
	public void testAddOnFinishedBlockingPartition() throws Exception {
		testAddOnFinishedPartition(ResultPartitionType.BLOCKING);
	}

	/**
	 * Tests {@link ResultPartition#writeBuffer} on a partition which has already finished.
	 *
	 * @param pipelined the result partition type to set up
	 */
	protected void testAddOnFinishedPartition(final ResultPartitionType pipelined)
		throws Exception {
		Buffer buffer = TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE);
		ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		try {
			ResultPartition partition = createPartition(notifier, pipelined, true);
			partition.finish();
			reset(notifier);
			// partition.add() should fail
			partition.writeBuffer(buffer, 0);
			Assert.fail("exception expected");
		} catch (IllegalStateException e) {
			// expected => ignored
		} finally {
			if (!buffer.isRecycled()) {
				Assert.fail("buffer not recycled");
				buffer.recycleBuffer();
			}
			// should not have notified either
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class), any(TaskActions.class));
		}
	}

	@Test
	public void testAddOnReleasedPipelinedPartition() throws Exception {
		testAddOnReleasedPartition(ResultPartitionType.PIPELINED);
	}

	@Test
	public void testAddOnReleasedBlockingPartition() throws Exception {
		testAddOnReleasedPartition(ResultPartitionType.BLOCKING);
	}

	/**
	 * Tests {@link ResultPartition#writeBuffer} on a partition which has already been released.
	 *
	 * @param pipelined the result partition type to set up
	 */
	protected void testAddOnReleasedPartition(final ResultPartitionType pipelined)
		throws Exception {
		Buffer buffer = TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE);
		ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		try {
			ResultPartition partition = createPartition(notifier, pipelined, true);
			partition.release();
			// partition.add() silently drops the buffer but recycles it
			partition.writeBuffer(buffer, 0);
		} finally {
			if (!buffer.isRecycled()) {
				Assert.fail("buffer not recycled");
				buffer.recycleBuffer();
			}
			// should not have notified either
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class), any(TaskActions.class));
		}
	}

	/**
	 * Tests that event buffers are properly added and recycled when broadcasting events
	 * to multiple channels.
	 */
	@Test
	public void testWriteBufferToAllSubpartitionsReferenceCounting() throws Exception {
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

		partition.writeBufferToAllSubpartitions(buffer);

		// release the buffers in the partition
		partition.release();

		assertTrue(buffer.isRecycled());
	}

	@Test
	public void testAddOnPipelinedPartition() throws Exception {
		testAddOnPartition(ResultPartitionType.PIPELINED);
	}

	@Test
	public void testAddOnBlockingPartition() throws Exception {
		testAddOnPartition(ResultPartitionType.BLOCKING);
	}

	/**
	 * Tests {@link ResultPartition#writeBuffer(Buffer, int)} on a working partition.
	 *
	 * @param pipelined the result partition type to set up
	 */
	protected void testAddOnPartition(final ResultPartitionType pipelined)
		throws Exception {
		ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		ResultPartition partition = createPartition(notifier, pipelined, true);
		Buffer buffer = TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE);
		try {
			// partition.add() adds the buffer without recycling it (if not spilling)
			partition.writeBuffer(buffer, 0);
			assertFalse("buffer should not be recycled (still in the queue)", buffer.isRecycled());
		} finally {
			if (!buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			// should have been notified for pipelined partitions
			if (pipelined.isPipelined()) {
				verify(notifier, times(1))
					.notifyPartitionConsumable(
						eq(partition.getJobId()),
						eq(partition.getPartitionId()),
						any(TaskActions.class));
			}
		}
	}

	// ------------------------------------------------------------------------

	private static ResultPartition createPartition(
		ResultPartitionConsumableNotifier notifier,
		ResultPartitionType type,
		boolean sendScheduleOrUpdateConsumersMessage) {
		return new ResultPartition(
			"TestTask",
			mock(TaskActions.class),
			new JobID(),
			new ResultPartitionID(),
			type,
			1,
			1,
			mock(ResultPartitionManager.class),
			notifier,
			ioManager,
			sendScheduleOrUpdateConsumersMessage);
	}
}

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
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
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
			partition.addBufferConsumer(createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE), 0);
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
			partition.addBufferConsumer(createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE), 0);
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class), any(TaskActions.class));
		}

		{
			// Blocking, send message => don't notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.BLOCKING, true);
			partition.addBufferConsumer(createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE), 0);
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class), any(TaskActions.class));
		}

		{
			// Blocking, don't send message => don't notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartition partition = createPartition(notifier, ResultPartitionType.BLOCKING, false);
			partition.addBufferConsumer(createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE), 0);
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
	 * Tests {@link ResultPartition#addBufferConsumer} on a partition which has already finished.
	 *
	 * @param pipelined the result partition type to set up
	 */
	protected void testAddOnFinishedPartition(final ResultPartitionType pipelined)
		throws Exception {
		BufferConsumer bufferConsumer = createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE);
		ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		try {
			ResultPartition partition = createPartition(notifier, pipelined, true);
			partition.finish();
			reset(notifier);
			// partition.add() should fail
			partition.addBufferConsumer(bufferConsumer, 0);
			Assert.fail("exception expected");
		} catch (IllegalStateException e) {
			// expected => ignored
		} finally {
			if (!bufferConsumer.isRecycled()) {
				bufferConsumer.close();
				Assert.fail("bufferConsumer not recycled");
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
	 * Tests {@link ResultPartition#addBufferConsumer} on a partition which has already been released.
	 *
	 * @param pipelined the result partition type to set up
	 */
	protected void testAddOnReleasedPartition(final ResultPartitionType pipelined)
		throws Exception {
		BufferConsumer bufferConsumer = createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE);
		ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		try {
			ResultPartition partition = createPartition(notifier, pipelined, true);
			partition.release();
			// partition.add() silently drops the bufferConsumer but recycles it
			partition.addBufferConsumer(bufferConsumer, 0);
		} finally {
			if (!bufferConsumer.isRecycled()) {
				bufferConsumer.close();
				Assert.fail("bufferConsumer not recycled");
			}
			// should not have notified either
			verify(notifier, never()).notifyPartitionConsumable(any(JobID.class), any(ResultPartitionID.class), any(TaskActions.class));
		}
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
	 * Tests {@link ResultPartition#addBufferConsumer(BufferConsumer, int)} on a working partition.
	 *
	 * @param pipelined the result partition type to set up
	 */
	protected void testAddOnPartition(final ResultPartitionType pipelined)
		throws Exception {
		ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		ResultPartition partition = createPartition(notifier, pipelined, true);
		BufferConsumer bufferConsumer = createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE);
		try {
			// partition.add() adds the bufferConsumer without recycling it (if not spilling)
			partition.addBufferConsumer(bufferConsumer, 0);
			assertFalse("bufferConsumer should not be recycled (still in the queue)", bufferConsumer.isRecycled());
		} finally {
			if (!bufferConsumer.isRecycled()) {
				bufferConsumer.close();
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

	@Test
	public void testReleaseMemoryOnBlockingPartition() throws Exception {
		testReleaseMemory(ResultPartitionType.BLOCKING);
	}

	@Test
	public void testReleaseMemoryOnPipelinedPartition() throws Exception {
		testReleaseMemory(ResultPartitionType.PIPELINED);
	}

	/**
	 * Tests {@link ResultPartition#releaseMemory(int)} on a working partition.
	 *
	 * @param resultPartitionType the result partition type to set up
	 */
	private void testReleaseMemory(final ResultPartitionType resultPartitionType) throws Exception {
		final int numBuffers = 10;
		final NetworkEnvironment network = new NetworkEnvironment(
			new NetworkBufferPool(numBuffers, 128),
			new LocalConnectionManager(),
			new ResultPartitionManager(),
			new TaskEventDispatcher(),
			new KvStateRegistry(),
			null,
			null,
			IOManager.IOMode.SYNC,
			0,
			0,
			2,
			8,
			true);
		final ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		final ResultPartition resultPartition = spy(createPartition(notifier, resultPartitionType, false));
		final List<Buffer> buffers = new ArrayList<>(numBuffers);
		try {
			network.setupPartition(resultPartition);

			// take all buffers (more than the minimum required)
			for (int i = 0; i < numBuffers; ++i) {
				Buffer buffer = resultPartition.getBufferPool().requestBuffer();
				buffers.add(buffer);
				assertNotNull(buffer);
			}

			// reset the pool size less than the number of requested buffers
			resultPartition.getBufferPool().setNumBuffers(4);

			// partition with blocking type should release excess buffers
			if (!resultPartitionType.hasBackPressure()) {
				verify(resultPartition, times(1)).releaseMemory(eq(numBuffers - 4));
			}
		} finally {
			for (Buffer buffer : buffers) {
				buffer.recycleBuffer();
			}
			resultPartition.release();
			network.shutdown();
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

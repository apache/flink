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
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.NetworkEnvironmentBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskmanager.ConsumableNotifyingResultPartitionWriterDecorator;
import org.apache.flink.runtime.taskmanager.NoOpTaskActions;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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

	/**
	 * Tests the schedule or update consumers message sending behaviour depending on the relevant flags.
	 */
	@Test
	public void testSendScheduleOrUpdateConsumersMessage() throws Exception {
		JobID jobId = new JobID();
		TaskActions taskActions = new NoOpTaskActions();

		{
			// Pipelined, send message => notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartitionWriter consumableNotifyingPartitionWriter = createConsumableNotifyingResultPartitionWriter(
				ResultPartitionType.PIPELINED,
				taskActions,
				jobId,
				notifier);
			consumableNotifyingPartitionWriter.addBufferConsumer(createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE), 0);
			verify(notifier, times(1))
				.notifyPartitionConsumable(eq(jobId), eq(consumableNotifyingPartitionWriter.getPartitionId()), eq(taskActions));
		}

		{
			// Blocking, send message => don't notify
			ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
			ResultPartitionWriter partition = createConsumableNotifyingResultPartitionWriter(
				ResultPartitionType.BLOCKING,
				taskActions,
				jobId,
				notifier);
			partition.addBufferConsumer(createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE), 0);
			verify(notifier, never()).notifyPartitionConsumable(eq(jobId), eq(partition.getPartitionId()), eq(taskActions));
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
	 * @param partitionType the result partition type to set up
	 */
	private void testAddOnFinishedPartition(final ResultPartitionType partitionType) throws Exception {
		BufferConsumer bufferConsumer = createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE);
		ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		JobID jobId = new JobID();
		TaskActions taskActions = new NoOpTaskActions();
		ResultPartitionWriter consumableNotifyingPartitionWriter = createConsumableNotifyingResultPartitionWriter(
			partitionType,
			taskActions,
			jobId,
			notifier);
		try {
			consumableNotifyingPartitionWriter.finish();
			reset(notifier);
			// partition.add() should fail
			consumableNotifyingPartitionWriter.addBufferConsumer(bufferConsumer, 0);
			Assert.fail("exception expected");
		} catch (IllegalStateException e) {
			// expected => ignored
		} finally {
			if (!bufferConsumer.isRecycled()) {
				bufferConsumer.close();
				Assert.fail("bufferConsumer not recycled");
			}
			// should not have notified either
			verify(notifier, never()).notifyPartitionConsumable(
				eq(jobId),
				eq(consumableNotifyingPartitionWriter.getPartitionId()),
				eq(taskActions));
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
	 * @param partitionType the result partition type to set up
	 */
	private void testAddOnReleasedPartition(final ResultPartitionType partitionType) throws Exception {
		BufferConsumer bufferConsumer = createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE);
		ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		JobID jobId = new JobID();
		TaskActions taskActions = new NoOpTaskActions();
		ResultPartition partition = createPartition(partitionType);
		ResultPartitionWriter consumableNotifyingPartitionWriter = ConsumableNotifyingResultPartitionWriterDecorator.decorate(
			Collections.singleton(createPartitionDeploymentDescriptor(partitionType)),
			new ResultPartitionWriter[] {partition},
			taskActions,
			jobId,
			notifier)[0];
		try {
			partition.release();
			// partition.add() silently drops the bufferConsumer but recycles it
			consumableNotifyingPartitionWriter.addBufferConsumer(bufferConsumer, 0);
			assertTrue(partition.isReleased());
		} finally {
			if (!bufferConsumer.isRecycled()) {
				bufferConsumer.close();
				Assert.fail("bufferConsumer not recycled");
			}
			// should not have notified either
			verify(notifier, never()).notifyPartitionConsumable(eq(jobId), eq(partition.getPartitionId()), eq(taskActions));
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
	 * @param partitionType the result partition type to set up
	 */
	private void testAddOnPartition(final ResultPartitionType partitionType) throws Exception {
		ResultPartitionConsumableNotifier notifier = mock(ResultPartitionConsumableNotifier.class);
		JobID jobId = new JobID();
		TaskActions taskActions = new NoOpTaskActions();
		ResultPartitionWriter consumableNotifyingPartitionWriter = createConsumableNotifyingResultPartitionWriter(
			partitionType,
			taskActions,
			jobId,
			notifier);
		BufferConsumer bufferConsumer = createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE);
		try {
			// partition.add() adds the bufferConsumer without recycling it (if not spilling)
			consumableNotifyingPartitionWriter.addBufferConsumer(bufferConsumer, 0);
			assertFalse("bufferConsumer should not be recycled (still in the queue)", bufferConsumer.isRecycled());
		} finally {
			if (!bufferConsumer.isRecycled()) {
				bufferConsumer.close();
			}
			// should have been notified for pipelined partitions
			if (partitionType.isPipelined()) {
				verify(notifier, times(1))
					.notifyPartitionConsumable(eq(jobId), eq(consumableNotifyingPartitionWriter.getPartitionId()), eq(taskActions));
			}
		}
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
		final int numAllBuffers = 10;
		final NetworkEnvironment network = new NetworkEnvironmentBuilder()
			.setNumNetworkBuffers(numAllBuffers).build();
		final ResultPartition resultPartition = createPartition(network, resultPartitionType, 1);
		try {
			resultPartition.setup();

			// take all buffers (more than the minimum required)
			for (int i = 0; i < numAllBuffers; ++i) {
				BufferBuilder bufferBuilder = resultPartition.getBufferPool().requestBufferBuilderBlocking();
				resultPartition.addBufferConsumer(bufferBuilder.createBufferConsumer(), 0);
			}
			resultPartition.finish();

			assertEquals(0, resultPartition.getBufferPool().getNumberOfAvailableMemorySegments());

			// reset the pool size less than the number of requested buffers
			final int numLocalBuffers = 4;
			resultPartition.getBufferPool().setNumBuffers(numLocalBuffers);

			// partition with blocking type should release excess buffers
			if (!resultPartitionType.hasBackPressure()) {
				assertEquals(numLocalBuffers, resultPartition.getBufferPool().getNumberOfAvailableMemorySegments());
			} else {
				assertEquals(0, resultPartition.getBufferPool().getNumberOfAvailableMemorySegments());
			}
		} finally {
			resultPartition.release();
			network.close();
		}
	}

	private ResultPartitionWriter createConsumableNotifyingResultPartitionWriter(
			ResultPartitionType partitionType,
			TaskActions taskActions,
			JobID jobId,
			ResultPartitionConsumableNotifier notifier) {
		return ConsumableNotifyingResultPartitionWriterDecorator.decorate(
			Collections.singleton(createPartitionDeploymentDescriptor(partitionType)),
			new ResultPartitionWriter[] {createPartition(partitionType)},
			taskActions,
			jobId,
			notifier)[0];
	}

	private ResultPartitionDeploymentDescriptor createPartitionDeploymentDescriptor(ResultPartitionType partitionType) {
		ShuffleDescriptor shuffleDescriptor = NettyShuffleDescriptorBuilder.newBuilder().buildLocal();
		PartitionDescriptor partitionDescriptor = new PartitionDescriptor(
			new IntermediateDataSetID(),
			shuffleDescriptor.getResultPartitionID().getPartitionId(),
			partitionType,
			1,
			0);
		return new ResultPartitionDeploymentDescriptor(
			partitionDescriptor,
			shuffleDescriptor,
			1,
			true);
	}
}

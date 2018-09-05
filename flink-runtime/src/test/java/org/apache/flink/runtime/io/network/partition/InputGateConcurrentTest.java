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
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createDummyConnectionManager;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createResultPartitionManager;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class InputGateConcurrentTest {

	@Test
	public void testConsumptionWithLocalChannels() throws Exception {
		final int numChannels = 11;
		final int buffersPerChannel = 1000;

		final ResultPartition resultPartition = mock(ResultPartition.class);

		final PipelinedSubpartition[] partitions = new PipelinedSubpartition[numChannels];
		final Source[] sources = new Source[numChannels];

		final ResultPartitionManager resultPartitionManager = createResultPartitionManager(partitions);

		final SingleInputGate gate = new SingleInputGate(
				"Test Task Name",
				new JobID(),
				new IntermediateDataSetID(), ResultPartitionType.PIPELINED,
				0, numChannels,
				mock(TaskActions.class),
				UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
				true);

		for (int i = 0; i < numChannels; i++) {
			LocalInputChannel channel = new LocalInputChannel(gate, i, new ResultPartitionID(),
					resultPartitionManager, mock(TaskEventDispatcher.class), UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
			gate.setInputChannel(new IntermediateResultPartitionID(), channel);

			partitions[i] = new PipelinedSubpartition(0, resultPartition);
			sources[i] = new PipelinedSubpartitionSource(partitions[i]);
		}

		ProducerThread producer = new ProducerThread(sources, numChannels * buffersPerChannel, 4, 10);
		ConsumerThread consumer = new ConsumerThread(gate, numChannels * buffersPerChannel);
		producer.start();
		consumer.start();

		// the 'sync()' call checks for exceptions and failed assertions
		producer.sync();
		consumer.sync();
	}

	@Test
	public void testConsumptionWithRemoteChannels() throws Exception {
		final int numChannels = 11;
		final int buffersPerChannel = 1000;

		final ConnectionManager connManager = createDummyConnectionManager();
		final Source[] sources = new Source[numChannels];

		final SingleInputGate gate = new SingleInputGate(
				"Test Task Name",
				new JobID(),
				new IntermediateDataSetID(), ResultPartitionType.PIPELINED,
				0,
				numChannels,
				mock(TaskActions.class),
				UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
				true);

		for (int i = 0; i < numChannels; i++) {
			RemoteInputChannel channel = new RemoteInputChannel(
					gate, i, new ResultPartitionID(), mock(ConnectionID.class),
					connManager, 0, 0, UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
			gate.setInputChannel(new IntermediateResultPartitionID(), channel);

			sources[i] = new RemoteChannelSource(channel);
		}

		ProducerThread producer = new ProducerThread(sources, numChannels * buffersPerChannel, 4, 10);
		ConsumerThread consumer = new ConsumerThread(gate, numChannels * buffersPerChannel);
		producer.start();
		consumer.start();

		// the 'sync()' call checks for exceptions and failed assertions
		producer.sync();
		consumer.sync();
	}

	@Test
	public void testConsumptionWithMixedChannels() throws Exception {
		final int numChannels = 61;
		final int numLocalChannels = 20;
		final int buffersPerChannel = 1000;

		// fill the local/remote decision
		List<Boolean> localOrRemote = new ArrayList<>(numChannels);
		for (int i = 0; i < numChannels; i++) {
			localOrRemote.add(i < numLocalChannels);
		}
		Collections.shuffle(localOrRemote);

		final ConnectionManager connManager = createDummyConnectionManager();
		final ResultPartition resultPartition = mock(ResultPartition.class);

		final PipelinedSubpartition[] localPartitions = new PipelinedSubpartition[numLocalChannels];
		final ResultPartitionManager resultPartitionManager = createResultPartitionManager(localPartitions);

		final Source[] sources = new Source[numChannels];

		final SingleInputGate gate = new SingleInputGate(
				"Test Task Name",
				new JobID(),
				new IntermediateDataSetID(), ResultPartitionType.PIPELINED,
				0,
				numChannels,
				mock(TaskActions.class),
				UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
				true);

		for (int i = 0, local = 0; i < numChannels; i++) {
			if (localOrRemote.get(i)) {
				// local channel
				PipelinedSubpartition psp = new PipelinedSubpartition(0, resultPartition);
				localPartitions[local++] = psp;
				sources[i] = new PipelinedSubpartitionSource(psp);

				LocalInputChannel channel = new LocalInputChannel(gate, i, new ResultPartitionID(),
						resultPartitionManager, mock(TaskEventDispatcher.class), UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
				gate.setInputChannel(new IntermediateResultPartitionID(), channel);
			}
			else {
				//remote channel
				RemoteInputChannel channel = new RemoteInputChannel(
						gate, i, new ResultPartitionID(), mock(ConnectionID.class),
						connManager, 0, 0, UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
				gate.setInputChannel(new IntermediateResultPartitionID(), channel);

				sources[i] = new RemoteChannelSource(channel);
			}
		}

		ProducerThread producer = new ProducerThread(sources, numChannels * buffersPerChannel, 4, 10);
		ConsumerThread consumer = new ConsumerThread(gate, numChannels * buffersPerChannel);
		producer.start();
		consumer.start();

		// the 'sync()' call checks for exceptions and failed assertions
		producer.sync();
		consumer.sync();
	}

	// ------------------------------------------------------------------------
	//  testing threads
	// ------------------------------------------------------------------------

	private static abstract class Source {
	
		abstract void addBufferConsumer(BufferConsumer bufferConsumer) throws Exception;
	}

	private static class PipelinedSubpartitionSource extends Source {

		final PipelinedSubpartition partition;

		PipelinedSubpartitionSource(PipelinedSubpartition partition) {
			this.partition = partition;
		}

		@Override
		void addBufferConsumer(BufferConsumer bufferConsumer) throws Exception {
			partition.add(bufferConsumer);
		}
	}

	private static class RemoteChannelSource extends Source {

		final RemoteInputChannel channel;
		private int seq = 0;

		RemoteChannelSource(RemoteInputChannel channel) {
			this.channel = channel;
		}

		@Override
		void addBufferConsumer(BufferConsumer bufferConsumer) throws Exception {
			checkState(bufferConsumer.isFinished(), "Handling of non finished buffers is not yet implemented");
			try {
				channel.onBuffer(bufferConsumer.build(), seq++, -1);
			}
			finally {
				bufferConsumer.close();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  testing threads
	// ------------------------------------------------------------------------

	private static class ProducerThread extends CheckedThread {

		private final Random rnd = new Random();
		private final Source[] sources;
		private final int numTotal;
		private final int maxChunk;
		private final int yieldAfter;

		ProducerThread(Source[] sources, int numTotal, int maxChunk, int yieldAfter) {
			this.sources = sources;
			this.numTotal = numTotal;
			this.maxChunk = maxChunk;
			this.yieldAfter = yieldAfter;
		}

		@Override
		public void go() throws Exception {
			final BufferConsumer bufferConsumer = BufferBuilderTestUtils.createFilledBufferConsumer(100);
			int nextYield = numTotal - yieldAfter;

			for (int i = numTotal; i > 0;) {
				final int nextChannel = rnd.nextInt(sources.length);
				final int chunk = Math.min(i, rnd.nextInt(maxChunk) + 1);

				final Source next = sources[nextChannel];

				for (int k = chunk; k > 0; --k) {
					next.addBufferConsumer(bufferConsumer.copy());
				}

				i -= chunk;

				if (i <= nextYield) {
					nextYield -= yieldAfter;
					//noinspection CallToThreadYield
					Thread.yield();
				}

			}
		}
	}

	private static class ConsumerThread extends CheckedThread {

		private final SingleInputGate gate;
		private final int numBuffers;

		ConsumerThread(SingleInputGate gate, int numBuffers) {
			this.gate = gate;
			this.numBuffers = numBuffers;
		}

		@Override
		public void go() throws Exception {
			for (int i = numBuffers; i > 0; --i) {
				assertNotNull(gate.getNextBufferOrEvent());
			}
		}
	}
}

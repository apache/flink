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

package org.apache.flink.runtime.io.network.partition.consumer;

import com.google.common.collect.Lists;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.io.network.util.TestPartitionProducer;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode.ASYNC;
import static org.mockito.Mockito.mock;

public class LocalInputChannelTest {

	/**
	 * Tests the consumption of multiple subpartitions via local input channels.
	 *
	 * <p> Multiple producer tasks produce pipelined partitions, which are consumed by multiple
	 * tasks via local input channels.
	 */
	@Test
	public void testConcurrentConsumeMultiplePartitions() throws Exception {
		// Config
		final int parallelism = 32;
		final int producerBufferPoolSize = parallelism + 1;
		final int numberOfBuffersPerChannel = 1024;

		checkArgument(parallelism >= 1);
		checkArgument(producerBufferPoolSize >= parallelism);
		checkArgument(numberOfBuffersPerChannel >= 1);

		// Setup
		// One thread per produced partition and one per consumer
		final ExecutorService executor = Executors.newFixedThreadPool(2 * parallelism);

		final NetworkBufferPool networkBuffers = new NetworkBufferPool(
				(parallelism * producerBufferPoolSize) + (parallelism * parallelism),
				TestBufferFactory.BUFFER_SIZE);

		final ResultPartitionConsumableNotifier partitionConsumableNotifier =
				mock(ResultPartitionConsumableNotifier.class);

		final IOManager ioManager = mock(IOManager.class);

		final JobID jobId = new JobID();

		final ResultPartitionManager partitionManager = new ResultPartitionManager();

		final ResultPartitionID[] partitionIds = new ResultPartitionID[parallelism];
		final TestPartitionProducer[] partitionProducers = new TestPartitionProducer[parallelism];

		// Create all partitions
		for (int i = 0; i < parallelism; i++) {
			partitionIds[i] = new ResultPartitionID();

			final ResultPartition partition = new ResultPartition(
					"Test Name",
					jobId,
					partitionIds[i],
					ResultPartitionType.PIPELINED,
					parallelism,
					partitionManager,
					partitionConsumableNotifier,
					ioManager,
					ASYNC);

			// Create a buffer pool for this partition
			partition.registerBufferPool(
					networkBuffers.createBufferPool(producerBufferPoolSize, true));

			// Create the producer
			partitionProducers[i] = new TestPartitionProducer(
					partition,
					false,
					new TestPartitionProducerBufferSource(
							parallelism,
							partition.getBufferProvider(),
							numberOfBuffersPerChannel)
			);

			// Register with the partition manager in order to allow the local input channels to
			// request their respective partitions.
			partitionManager.registerResultPartition(partition);
		}

		// Test
		try {
			// Submit producer tasks
			List<Future<?>> results = Lists.newArrayListWithCapacity(
					parallelism + 1);

			for (int i = 0; i < parallelism; i++) {
				results.add(executor.submit(partitionProducers[i]));
			}

			// Submit consumer
			for (int i = 0; i < parallelism; i++) {
				results.add(executor.submit(
						new TestLocalInputChannelConsumer(
								i,
								parallelism,
								numberOfBuffersPerChannel,
								networkBuffers.createBufferPool(parallelism, true),
								partitionManager,
								new TaskEventDispatcher(),
								partitionIds)));
			}

			// Wait for all to finish
			for (Future<?> result : results) {
				result.get();
			}
		}
		finally {
			networkBuffers.destroy();
			executor.shutdown();
		}
	}

	/**
	 * Returns the configured number of buffers for each channel in a random order.
	 */
	private static class TestPartitionProducerBufferSource implements TestProducerSource {

		private final BufferProvider bufferProvider;

		private final List<Byte> channelIndexes;

		public TestPartitionProducerBufferSource(
				int parallelism,
				BufferProvider bufferProvider,
				int numberOfBuffersToProduce) {

			this.bufferProvider = bufferProvider;
			this.channelIndexes = Lists.newArrayListWithCapacity(
					parallelism * numberOfBuffersToProduce);

			// Array of channel indexes to produce buffers for
			for (byte i = 0; i < parallelism; i++) {
				for (int j = 0; j < numberOfBuffersToProduce; j++) {
					channelIndexes.add(i);
				}
			}

			// Random buffer to channel ordering
			Collections.shuffle(channelIndexes);
		}

		@Override
		public BufferOrEvent getNextBufferOrEvent() throws Exception {
			if (channelIndexes.size() > 0) {
				final int channelIndex = channelIndexes.remove(0);

				return new BufferOrEvent(bufferProvider.requestBufferBlocking(), channelIndex);
			}

			return null;
		}
	}

	/**
	 * Consumed the configured result partitions and verifies that each channel receives the
	 * expected number of buffers.
	 */
	private static class TestLocalInputChannelConsumer implements Callable<Void> {

		private final SingleInputGate inputGate;

		private final int numberOfInputChannels;

		private final int numberOfExpectedBuffersPerChannel;

		public TestLocalInputChannelConsumer(
				int subpartitionIndex,
				int numberOfInputChannels,
				int numberOfExpectedBuffersPerChannel,
				BufferPool bufferPool,
				ResultPartitionManager partitionManager,
				TaskEventDispatcher taskEventDispatcher,
				ResultPartitionID[] consumedPartitionIds) {

			checkArgument(numberOfInputChannels >= 1);
			checkArgument(numberOfExpectedBuffersPerChannel >= 1);

			this.inputGate = new SingleInputGate(
					"Test Name",
					new JobID(),
					new ExecutionAttemptID(),
					new IntermediateDataSetID(),
					subpartitionIndex,
					numberOfInputChannels,
					mock(PartitionStateChecker.class));

			// Set buffer pool
			inputGate.setBufferPool(bufferPool);

			// Setup input channels
			for (int i = 0; i < numberOfInputChannels; i++) {
				inputGate.setInputChannel(
						new IntermediateResultPartitionID(),
						new LocalInputChannel(
								inputGate,
								i,
								consumedPartitionIds[i],
								partitionManager,
								taskEventDispatcher));
			}

			this.numberOfInputChannels = numberOfInputChannels;
			this.numberOfExpectedBuffersPerChannel = numberOfExpectedBuffersPerChannel;
		}

		@Override
		public Void call() throws Exception {
			// One counter per input channel. Expect the same number of buffers from each channel.
			final int[] numberOfBuffersPerChannel = new int[numberOfInputChannels];

			try {
				BufferOrEvent boe;
				while ((boe = inputGate.getNextBufferOrEvent()) != null) {
					if (boe.isBuffer()) {
						boe.getBuffer().recycle();

						// Check that we don't receive too many buffers
						if (++numberOfBuffersPerChannel[boe.getChannelIndex()]
								> numberOfExpectedBuffersPerChannel) {

							throw new IllegalStateException("Received more buffers than expected " +
									"on channel " + boe.getChannelIndex() + ".");
						}
					}
				}

				// Verify that we received the expected number of buffers on each channel
				for (int i = 0; i < numberOfBuffersPerChannel.length; i++) {
					final int actualNumberOfReceivedBuffers = numberOfBuffersPerChannel[i];

					if (actualNumberOfReceivedBuffers != numberOfExpectedBuffersPerChannel) {
						throw new IllegalStateException("Received unexpected number of buffers " +
								"on channel " + i + " (" + actualNumberOfReceivedBuffers + " instead " +
								"of " + numberOfExpectedBuffersPerChannel + ").");
					}
				}
			}
			finally {
				inputGate.releaseAllResources();
			}

			return null;
		}
	}
}

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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link ResultPartition} which writes buffers directly to {@link ResultSubpartition}s. This
 * is in contrast to implementations where records are written to a joint structure, from which
 * the subpartitions draw the data after the write phase is finished, for example the sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all subpartitions return buffers (and backlog) to be
 * transported through the network.
 */
public abstract class BufferWritingResultPartition extends ResultPartition {

	/** The subpartitions of this partition. At least one. */
	protected final ResultSubpartition[] subpartitions;

	/** For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be null. */
	private final BufferBuilder[] subpartitionBufferBuilders;

	/** For broadcast mode, a single BufferBuilder is shared by all subpartitions. */
	private BufferBuilder broadcastBufferBuilder;

	private Meter idleTimeMsPerSecond = new MeterView(new SimpleCounter());

	public BufferWritingResultPartition(
		String owningTaskName,
		int partitionIndex,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		ResultSubpartition[] subpartitions,
		int numTargetKeyGroups,
		ResultPartitionManager partitionManager,
		@Nullable BufferCompressor bufferCompressor,
		FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {

		super(
			owningTaskName,
			partitionIndex,
			partitionId,
			partitionType,
			subpartitions.length,
			numTargetKeyGroups,
			partitionManager,
			bufferCompressor,
			bufferPoolFactory);

		this.subpartitions = checkNotNull(subpartitions);
		this.subpartitionBufferBuilders = new BufferBuilder[subpartitions.length];
	}

	public int getNumberOfQueuedBuffers() {
		int totalBuffers = 0;

		for (ResultSubpartition subpartition : subpartitions) {
			totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
		}

		return totalBuffers;
	}

	public int getNumberOfQueuedBuffers(int targetSubpartition) {
		checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
		return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
	}

	protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
		if (finishProducers) {
			finishBroadcastBufferBuilder();
			finishSubpartitionBufferBuilder(targetSubpartition);
		}

		subpartitions[targetSubpartition].flush();
	}

	protected void flushAllSubpartitions(boolean finishProducers) {
		if (finishProducers) {
			finishBroadcastBufferBuilder();
			finishSubpartitionBufferBuilders();
		}

		for (ResultSubpartition subpartition : subpartitions) {
			subpartition.flush();
		}
	}

	public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
		do {
			final BufferBuilder bufferBuilder = getSubpartitionBufferBuilder(targetSubpartition);
			bufferBuilder.appendAndCommit(record);

			if (bufferBuilder.isFull()) {
				finishSubpartitionBufferBuilder(targetSubpartition);
			}
		} while (record.hasRemaining());
	}

	@Override
	public void broadcastRecord(ByteBuffer record) throws IOException {
		do {
			final BufferBuilder bufferBuilder = getBroadcastBufferBuilder();
			bufferBuilder.appendAndCommit(record);

			if (bufferBuilder.isFull()) {
				finishBroadcastBufferBuilder();
			}
		} while (record.hasRemaining());
	}

	@Override
	public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
		checkInProduceState();
		finishBroadcastBufferBuilder();
		finishSubpartitionBufferBuilders();

		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event, isPriorityEvent)) {
			for (ResultSubpartition subpartition : subpartitions) {
				// Retain the buffer so that it can be recycled by each channel of targetPartition
				subpartition.add(eventBufferConsumer.copy());
			}
		}
	}

	@Override
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		super.setMetricGroup(metrics);
		idleTimeMsPerSecond = metrics.getIdleTimeMsPerSecond();
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener) throws IOException {
		checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
		checkState(!isReleased(), "Partition released.");

		ResultSubpartition subpartition = subpartitions[subpartitionIndex];
		ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);

		LOG.debug("Created {}", readView);

		return readView;
	}

	@Override
	public void finish() throws IOException {
		finishBroadcastBufferBuilder();
		finishSubpartitionBufferBuilders();

		for (ResultSubpartition subpartition : subpartitions) {
			subpartition.finish();
		}

		super.finish();
	}

	@Override
	protected void releaseInternal() {
		// Release all subpartitions
		for (ResultSubpartition subpartition : subpartitions) {
			try {
				subpartition.release();
			}
			// Catch this in order to ensure that release is called on all subpartitions
			catch (Throwable t) {
				LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
			}
		}
	}

	private BufferBuilder getSubpartitionBufferBuilder(int targetSubpartition) throws IOException {
		final BufferBuilder bufferBuilder = subpartitionBufferBuilders[targetSubpartition];
		if (bufferBuilder != null) {
			return bufferBuilder;
		}

		return getNewSubpartitionBufferBuilder(targetSubpartition);
	}

	private BufferBuilder getNewSubpartitionBufferBuilder(int targetSubpartition) throws IOException {
		checkInProduceState();
		ensureUnicastMode();

		final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
		subpartitions[targetSubpartition].add(bufferBuilder.createBufferConsumer());
		subpartitionBufferBuilders[targetSubpartition] = bufferBuilder;
		return bufferBuilder;
	}

	private BufferBuilder getBroadcastBufferBuilder() throws IOException {
		if (broadcastBufferBuilder != null) {
			return broadcastBufferBuilder;
		}

		return getNewBroadcastBufferBuilder();
	}

	private BufferBuilder getNewBroadcastBufferBuilder() throws IOException {
		checkInProduceState();
		ensureBroadcastMode();

		final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
		broadcastBufferBuilder = bufferBuilder;

		try (final BufferConsumer consumer = bufferBuilder.createBufferConsumer()) {
			for (ResultSubpartition subpartition : subpartitions) {
				subpartition.add(consumer.copy());
			}
		}

		return bufferBuilder;
	}

	private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition) throws IOException {
		BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
		if (bufferBuilder != null) {
			return bufferBuilder;
		}

		final long start = System.currentTimeMillis();
		try {
			bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
			idleTimeMsPerSecond.markEvent(System.currentTimeMillis() - start);
			return bufferBuilder;
		} catch (InterruptedException e) {
			throw new IOException("Interrupted while waiting for buffer");
		}
	}

	private void finishSubpartitionBufferBuilder(int targetSubpartition) {
		final BufferBuilder bufferBuilder = subpartitionBufferBuilders[targetSubpartition];
		if (bufferBuilder != null) {
			numBytesOut.inc(bufferBuilder.finish());
			numBuffersOut.inc();
			subpartitionBufferBuilders[targetSubpartition] = null;
		}
	}

	private void finishSubpartitionBufferBuilders() {
		for (int channelIndex = 0; channelIndex < numSubpartitions; channelIndex++) {
			finishSubpartitionBufferBuilder(channelIndex);
		}
	}

	private void finishBroadcastBufferBuilder() {
		if (broadcastBufferBuilder != null) {
			numBytesOut.inc(broadcastBufferBuilder.finish() * numSubpartitions);
			numBuffersOut.inc(numSubpartitions);
			broadcastBufferBuilder = null;
		}
	}

	private void ensureUnicastMode() {
		finishBroadcastBufferBuilder();
	}

	private void ensureBroadcastMode() {
		finishSubpartitionBufferBuilders();
	}

	@VisibleForTesting
	public Meter getIdleTimeMsPerSecond() {
		return idleTimeMsPerSecond;
	}

	@VisibleForTesting
	public ResultSubpartition[] getAllPartitions() {
		return subpartitions;
	}
}

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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.metrics.SumAndCount;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.taskmanager.TaskManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * InternalResultPartition is used when shuffling data through taskmanager.
 */
public class InternalResultPartition<T> extends ResultPartition<T> implements BufferPoolOwner {

	private static final Logger LOG = LoggerFactory.getLogger(InternalResultPartition.class);

	private final TaskActions taskActions;

	private final RecordSerializer serializer;

	private final Optional<BufferBuilder>[] bufferBuilders;

	/** The subpartitions of this partition. At least one. */
	private final ResultSubpartition[] subpartitions;

	private final ResultPartitionManager partitionManager;

	private final ResultPartitionConsumableNotifier partitionConsumableNotifier;

	private final boolean sendScheduleOrUpdateConsumersMessage;

	/**
	 * The total number of references to subpartitions of this result. The result partition can be
	 * safely released, iff the reference count is zero. A reference count of -1 denotes that the
	 * result partition has been released.
	 */
	private final AtomicInteger pendingReferences = new AtomicInteger();

	private BufferPool bufferPool;

	private boolean hasNotifiedPipelinedConsumers;

	/** enable tracing metrics */
	private boolean enableTracingMetrics;

	private int tracingMetricsInterval;

	private long resultCounter = 0L;

	/** wait buffer metrics */
	private SumAndCount nsWaitBufferTime;

	public InternalResultPartition(
		String owningTaskName,
		TaskActions taskActions, // actions on the owning task
		JobID jobId,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		int numberOfSubpartitions,
		int numTargetKeyGroups,
		ResultPartitionManager partitionManager,
		ResultPartitionConsumableNotifier partitionConsumableNotifier,
		IOManager ioManager,
		boolean sendScheduleOrUpdateConsumersMessage) {
		super(owningTaskName, jobId, partitionId, partitionType, numberOfSubpartitions, numTargetKeyGroups);

		this.taskActions = checkNotNull(taskActions);
		this.subpartitions = new ResultSubpartition[numberOfSubpartitions];
		this.partitionManager = checkNotNull(partitionManager);
		this.partitionConsumableNotifier = checkNotNull(partitionConsumableNotifier);
		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;
		this.serializer = new SpanningRecordSerializer<>();

		this.bufferBuilders = new Optional[numberOfSubpartitions];
		for (int i = 0; i < numberOfSubpartitions; i++) {
			bufferBuilders[i] = Optional.empty();
		}

		// Create the subpartitions.
		switch (partitionType) {
			case BLOCKING:
				for (int i = 0; i < subpartitions.length; i++) {
					subpartitions[i] = new SpillableSubpartition(i, this, ioManager);
				}

				break;

			case PIPELINED:
				for (int i = 0; i < subpartitions.length; i++) {
					subpartitions[i] = new PipelinedSubpartition(i, this);
				}

				break;

			default:
				throw new IllegalArgumentException("Unsupported result partition type.");
		}

		// Initially, partitions should be consumed once before release.
		pin();

		LOG.debug("{}: Initialized {}", owningTaskName, this);
	}

	/**
	 * Registers a buffer pool with this result partition.
	 * <p>
	 * There is one pool for each result partition, which is shared by all its sub partitions.
	 * <p>
	 * The pool is registered with the partition *after* it as been constructed in order to conform
	 * to the life-cycle of task registrations in the {@link TaskManager}.
	 */
	public void registerBufferPool(BufferPool bufferPool) {
		checkArgument(bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
				"Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for this result partition.");

		checkState(this.bufferPool == null, "Bug in result partition setup logic: Already registered buffer pool.");

		this.bufferPool = checkNotNull(bufferPool);

		// If the partition type is back pressure-free, we register with the buffer pool for
		// callbacks to release memory.
		if (!partitionType.hasBackPressure()) {
			bufferPool.setBufferPoolOwner(this);
		}
	}

	@VisibleForTesting
	public BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public int getNumberOfQueuedBuffers() {
		int totalBuffers = 0;

		for (ResultSubpartition subpartition : subpartitions) {
			totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
		}

		return totalBuffers;
	}

	public boolean getHasNotifiedPipelinedConsumers() {
		return hasNotifiedPipelinedConsumers;
	}

	// ------------------------------------------------------------------------

	@Override
	public void emitRecord(T record, int[] targetChannels, boolean isBroadcast, boolean flushAlways) throws IOException, InterruptedException {
		serializationDelegate.setInstance(record);

		serializer.serializeRecord(serializationDelegate);

		boolean pruneAfterCopying = false;

		if (isBroadcast) {
			pruneAfterCopying = copyFromSerializerToTargetChannel(0, true, flushAlways);
		} else {
			for (int channel : targetChannels) {
				if (copyFromSerializerToTargetChannel(channel, false, flushAlways)) {
					pruneAfterCopying = true;
				}
			}
		}

		// Make sure we don't hold onto the large intermediate serialization buffer for too long
		if (pruneAfterCopying) {
			serializer.prune();
		}
	}

	@Override
	public void emitRecord(T record, int targetChannel, boolean isBroadcast, boolean flushAlways) throws IOException, InterruptedException {
		serializationDelegate.setInstance(record);

		serializer.serializeRecord(serializationDelegate);

		if (isBroadcast) {
			tryFinishCurrentBufferBuilder(targetChannel, true);
		}

		if (copyFromSerializerToTargetChannel(targetChannel, isBroadcast, flushAlways)) {
			serializer.prune();
		}

		if (isBroadcast) {
			tryFinishCurrentBufferBuilder(targetChannel, true);
		}
	}

	@Override
	public void broadcastEvent(AbstractEvent event, boolean flushAlways) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int targetChannel = 0; targetChannel < numberOfSubpartitions; targetChannel++) {
				tryFinishCurrentBufferBuilder(targetChannel, false);

				// Retain the buffer so that it can be recycled by each channel of targetPartition
				addBufferConsumer(eventBufferConsumer.copy(), targetChannel);
			}

			if (flushAlways) {
				flushAll();
			}
		}
	}

	@VisibleForTesting
	public void addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException {
		checkNotNull(bufferConsumer);

		ResultSubpartition subpartition;
		try {
			checkInProduceState();
			subpartition = subpartitions[subpartitionIndex];
		}
		catch (Exception ex) {
			bufferConsumer.close();
			throw ex;
		}

		if (subpartition.add(bufferConsumer)) {
			notifyPipelinedConsumers();
		}
	}

	@Override
	public void flushAll() {
		for (ResultSubpartition subpartition : subpartitions) {
			subpartition.flush();
		}
	}

	@Override
	public void flush(int subpartitionIndex) {
		subpartitions[subpartitionIndex].flush();
	}

	@Override
	public void finish() throws IOException {
		boolean success = false;

		try {
			checkInProduceState();

			for (ResultSubpartition subpartition : subpartitions) {
				subpartition.finish();
			}

			success = true;
		}
		finally {
			if (success) {
				isFinished = true;

				notifyPipelinedConsumers();
			}
		}
	}

	public void destroyBufferPool() {
		if (bufferPool != null) {
			bufferPool.lazyDestroy();
		}
	}

	/**
	 * Returns the requested subpartition.
	 */
	public ResultSubpartitionView createSubpartitionView(int index, BufferAvailabilityListener availabilityListener) throws IOException {
		int refCnt = pendingReferences.get();

		checkState(refCnt != -1, "Partition released.");
		checkState(refCnt > 0, "Partition not pinned.");

		checkElementIndex(index, subpartitions.length, "Subpartition not found.");

		ResultSubpartitionView readView = subpartitions[index].createReadView(availabilityListener);

		LOG.debug("Created {}", readView);

		return readView;
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

		if (partitionType == ResultPartitionType.BLOCKING && bufferPool != null) {
			bufferPool.notifyBufferPoolOwnerReleased();
		}
	}

	/**
	 * Releases buffers held by this result partition.
	 *
	 * <p> This is a callback from the buffer pool, which is registered for result partitions, which
	 * are back pressure-free.
	 */
	@Override
	public void releaseMemory(int toRelease) throws IOException {
		checkArgument(toRelease > 0);

		for (ResultSubpartition subpartition : subpartitions) {
			toRelease -= subpartition.releaseMemory();

			// Only release as much memory as needed
			if (toRelease <= 0) {
				break;
			}
		}
	}

	@Override
	public String toString() {
		return "InternalResultPartition " + partitionId.toString() + " [" + partitionType + ", "
				+ subpartitions.length + " subpartitions, "
				+ pendingReferences + " pending references]";
	}

	// ------------------------------------------------------------------------

	/**
	 * Pins the result partition.
	 *
	 * <p> The partition can only be released after each subpartition has been consumed once per pin
	 * operation.
	 */
	void pin() {
		while (true) {
			int refCnt = pendingReferences.get();

			if (refCnt >= 0) {
				if (pendingReferences.compareAndSet(refCnt, refCnt + subpartitions.length)) {
					break;
				}
			}
			else {
				throw new IllegalStateException("Released.");
			}
		}
	}

	/**
	 * Notification when a subpartition is released.
	 */
	void onConsumedSubpartition(int subpartitionIndex) {

		if (isReleased.get()) {
			return;
		}

		int refCnt = pendingReferences.decrementAndGet();

		if (refCnt == 0) {
			partitionManager.onConsumedPartition(this);
		}
		else if (refCnt < 0) {
			throw new IllegalStateException("All references released.");
		}

		LOG.debug("{}: Received release notification for subpartition {} (reference count now at: {}).",
				this, subpartitionIndex, pendingReferences);
	}

	ResultSubpartition[] getAllPartitions() {
		return subpartitions;
	}

	/**
	 * Notifies pipelined consumers of this result partition once.
	 */
	private void notifyPipelinedConsumers() {
		if (sendScheduleOrUpdateConsumersMessage && !hasNotifiedPipelinedConsumers && partitionType.isPipelined()) {
			partitionConsumableNotifier.notifyPartitionConsumable(jobId, partitionId, taskActions);

			hasNotifiedPipelinedConsumers = true;
		}
	}

	@Nonnull
	private BufferBuilder requestNewBufferBuilder(int targetChannel, boolean isBroadcast) throws IOException, InterruptedException {
		checkState(!bufferBuilders[targetChannel].isPresent() || bufferBuilders[targetChannel].get().isFinished());

		final BufferBuilder bufferBuilder;
		if (enableTracingMetrics && (resultCounter++ % tracingMetricsInterval == 0)) {
			final long start = System.nanoTime();
			bufferBuilder = getBufferProvider().requestBufferBuilderBlocking();
			nsWaitBufferTime.update(System.nanoTime() - start);
		} else {
			bufferBuilder = getBufferProvider().requestBufferBuilderBlocking();
		}

		if (isBroadcast) {
			BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
			addBufferConsumer(bufferConsumer, 0);
			bufferBuilders[0] = Optional.of(bufferBuilder);

			// copy the buffer consumer to other channels
			for (int channel = 1; channel < numberOfSubpartitions; channel++) {
				checkState(!bufferBuilders[channel].isPresent() || bufferBuilders[channel].get().isFinished());
				addBufferConsumer(bufferConsumer.copy(), channel);
				bufferBuilders[channel] = Optional.of(bufferBuilder);
			}
		} else {
			addBufferConsumer(bufferBuilder.createBufferConsumer(), targetChannel);
			bufferBuilders[targetChannel] = Optional.of(bufferBuilder);
		}

		return bufferBuilder;
	}

	/**
	 * The {@link BufferBuilder} may already exist if not filled up last time, otherwise we need
	 * request a new one for this target channel.
	 */
	@Nonnull
	private BufferBuilder getBufferBuilder(int targetChannel, boolean isBroadcast) throws IOException, InterruptedException {
		if (bufferBuilders[targetChannel].isPresent() && !bufferBuilders[targetChannel].get().isFinished()) {
			return bufferBuilders[targetChannel].get();
		} else {
			return requestNewBufferBuilder(targetChannel, isBroadcast);
		}
	}

	/**
	 * Marks the current {@link BufferBuilder} as finished if exists.
	 */
	private void tryFinishCurrentBufferBuilder(int targetChannel, boolean isBroadcast) {
		if (!bufferBuilders[targetChannel].isPresent()) {
			return;
		}

		BufferBuilder bufferBuilder = bufferBuilders[targetChannel].get();
		updateMetrics(bufferBuilder, isBroadcast);
	}

	private void updateMetrics(BufferBuilder bufferBuilder, boolean isBroadcast) {
		if (isBroadcast) {
			numBytesOut.inc((long) bufferBuilder.finish() * numberOfSubpartitions);
			numBuffersOut.inc(numberOfSubpartitions);
		} else {
			numBytesOut.inc(bufferBuilder.finish());
			numBuffersOut.inc();
		}
	}

	private boolean copyFromSerializerToTargetChannel(int targetChannel, boolean isBroadcast, boolean flushAlways) throws IOException, InterruptedException {
		// We should reset the initial position of the intermediate serialization data buffer before
		// copying, so the serialization results can be copied to many different target buffers.
		serializer.reset();

		boolean pruneTriggered = false;

		BufferBuilder bufferBuilder = getBufferBuilder(targetChannel, isBroadcast);
		RecordSerializer.SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
		while (result.isFullBuffer()) {
			updateMetrics(bufferBuilder, isBroadcast);

			// If this was a full record, we are done. Not breaking
			// out of the loop at this point will lead to another
			// buffer request before breaking out (that would not be
			// a problem per se, but it can lead to stalls in the pipeline).
			if (result.isFullRecord()) {
				pruneTriggered = true;
				break;
			}

			bufferBuilder = requestNewBufferBuilder(targetChannel, isBroadcast);
			result = serializer.copyToBufferBuilder(bufferBuilder);
		}

		checkState(!serializer.hasSerializedData(), "All data should be written at once");

		if (flushAlways) {
			if (isBroadcast) {
				flushAll();
			} else {
				flush(targetChannel);
			}
		}

		return pruneTriggered;
	}

	private void closeBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel].isPresent()) {
			bufferBuilders[targetChannel].get().finish();
			bufferBuilders[targetChannel] = Optional.empty();
		}
	}

	@Override
	public void clearBuffers() {
		for (int targetChannel = 0; targetChannel < subpartitions.length; targetChannel++) {
			closeBufferBuilder(targetChannel);
		}
	}

	@Override
	public void setTypeSerializer(TypeSerializer typeSerializer) {
		super.setTypeSerializer(typeSerializer);
		serializationDelegate = new SerializationDelegate<>(typeSerializer);
	}

	@Override
	public void setMetricGroup(TaskIOMetricGroup metrics, boolean enableTracingMetrics, int tracingMetricsInterval) {
		super.setMetricGroup(metrics, enableTracingMetrics, tracingMetricsInterval);

		this.enableTracingMetrics = enableTracingMetrics;
		this.tracingMetricsInterval = tracingMetricsInterval;

		if (enableTracingMetrics) {
			this.nsWaitBufferTime = metrics.getNsWaitBufferTime();
		}
	}
}

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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An abstract record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flushAll()} after
 * all records have been written with {@link #emit(IOReadableWritable)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public abstract class RecordWriter<T extends IOReadableWritable> implements AvailabilityProvider {
	private final ResultPartitionWriter targetPartition;

	protected final int numberOfChannels;

	protected final RecordSerializer<T> serializer;

	protected final Random rng = new XORShiftRandom();

	private final boolean flushAlways;

	private Counter numBytesOut = new SimpleCounter();

	private Counter numBuffersOut = new SimpleCounter();

	protected Meter idleTimeMsPerSecond = new MeterView(new SimpleCounter());

	RecordWriter(ResultPartitionWriter writer, boolean flushAlways) {
		this.targetPartition = writer;
		this.numberOfChannels = writer.getNumberOfSubpartitions();

		this.serializer = new SpanningRecordSerializer<T>();

		this.flushAlways = flushAlways;
	}

	protected void emit(T record, int targetChannel) throws IOException, InterruptedException {
		serializer.serializeRecord(record);

		// Make sure we don't hold onto the large intermediate serialization buffer for too long
		if (copyFromSerializerToTargetChannel(targetChannel)) {
			serializer.prune();
		}
	}

	/**
	 * @param targetChannel
	 * @return <tt>true</tt> if the intermediate serialization buffer should be pruned
	 */
	protected boolean copyFromSerializerToTargetChannel(int targetChannel) throws IOException, InterruptedException {
		// We should reset the initial position of the intermediate serialization buffer before
		// copying, so the serialization results can be copied to multiple target buffers.
		serializer.reset();

		boolean pruneTriggered = false;
		BufferBuilder bufferBuilder = getBufferBuilder(targetChannel);
		SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
		while (result.isFullBuffer()) {
			finishBufferBuilder(bufferBuilder);

			// If this was a full record, we are done. Not breaking out of the loop at this point
			// will lead to another buffer request before breaking out (that would not be a
			// problem per se, but it can lead to stalls in the pipeline).
			if (result.isFullRecord()) {
				pruneTriggered = true;
				emptyCurrentBufferBuilder(targetChannel);
				break;
			}

			bufferBuilder = requestNewBufferBuilder(targetChannel);
			result = serializer.copyToBufferBuilder(bufferBuilder);
		}
		checkState(!serializer.hasSerializedData(), "All data should be written at once");

		if (flushAlways) {
			flushTargetPartition(targetChannel);
		}
		return pruneTriggered;
	}

	public void broadcastEvent(AbstractEvent event) throws IOException {
		broadcastEvent(event, false);
	}

	public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int targetChannel = 0; targetChannel < numberOfChannels; targetChannel++) {
				tryFinishCurrentBufferBuilder(targetChannel);

				// Retain the buffer so that it can be recycled by each channel of targetPartition
				targetPartition.addBufferConsumer(eventBufferConsumer.copy(), targetChannel, isPriorityEvent);
			}

			if (flushAlways) {
				flushAll();
			}
		}
	}

	public void flushAll() {
		targetPartition.flushAll();
	}

	protected void flushTargetPartition(int targetChannel) {
		targetPartition.flush(targetChannel);
	}

	/**
	 * Sets the metric group for this RecordWriter.
     */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		numBytesOut = metrics.getNumBytesOutCounter();
		numBuffersOut = metrics.getNumBuffersOutCounter();
		idleTimeMsPerSecond = metrics.getIdleTimeMsPerSecond();
	}

	protected void finishBufferBuilder(BufferBuilder bufferBuilder) {
		numBytesOut.inc(bufferBuilder.finish());
		numBuffersOut.inc();
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return targetPartition.getAvailableFuture();
	}

	/**
	 * This is used to send regular records.
	 */
	public abstract void emit(T record) throws IOException, InterruptedException;

	/**
	 * This is used to send LatencyMarks to a random target channel.
	 */
	public abstract void randomEmit(T record) throws IOException, InterruptedException;

	/**
	 * This is used to broadcast streaming Watermarks in-band with records.
	 */
	public abstract void broadcastEmit(T record) throws IOException, InterruptedException;

	/**
	 * The {@link BufferBuilder} may already exist if not filled up last time, otherwise we need
	 * request a new one for this target channel.
	 */
	@VisibleForTesting
	public abstract BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException;

	/**
	 * Marks the current {@link BufferBuilder} as finished if present and clears the state for next one.
	 */
	abstract void tryFinishCurrentBufferBuilder(int targetChannel);

	/**
	 * Marks the current {@link BufferBuilder} as empty for the target channel.
	 */
	abstract void emptyCurrentBufferBuilder(int targetChannel);

	/**
	 * Marks the current {@link BufferBuilder} as finished and releases the resources for the target channel.
	 */
	abstract void closeBufferBuilder(int targetChannel);

	/**
	 * Closes the {@link BufferBuilder}s for all the channels.
	 */
	public abstract void clearBuffers();

	/**
	 * Closes the writer. This stops the flushing thread (if there is one).
	 */
	public void close() {
		clearBuffers();
	}

	protected void addBufferConsumer(BufferConsumer consumer, int targetChannel) throws IOException {
		targetPartition.addBufferConsumer(consumer, targetChannel);
	}

	/**
	 * Requests a new {@link BufferBuilder} for the target channel and returns it.
	 */
	public BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		BufferBuilder builder = targetPartition.tryGetBufferBuilder(targetChannel);
		if (builder == null) {
			long start = System.currentTimeMillis();
			builder = targetPartition.getBufferBuilder(targetChannel);
			idleTimeMsPerSecond.markEvent(System.currentTimeMillis() - start);
		}
		return builder;
	}

	@VisibleForTesting
	public Meter getIdleTimeMsPerSecond() {
		return idleTimeMsPerSecond;
	}

	@VisibleForTesting
	public ResultPartitionWriter getTargetPartition() {
		return targetPartition;
	}
}

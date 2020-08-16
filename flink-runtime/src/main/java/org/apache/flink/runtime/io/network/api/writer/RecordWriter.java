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
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * records serialization.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends IOReadableWritable> implements AvailabilityProvider {

	/** Default name for the output flush thread, if no name with a task reference is given. */
	@VisibleForTesting
	public static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

	private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

	private final ResultPartitionWriter targetPartition;

	private final ChannelSelector<T> channelSelector;

	private boolean isBroadcastSelector;

	private final int numberOfChannels;

	private final DataOutputSerializer serializer;

	private final Random rng = new XORShiftRandom();

	private final boolean flushAlways;

	/** The thread that periodically flushes the output, to give an upper latency bound. */
	@Nullable
	private final OutputFlusher outputFlusher;

	/** To avoid synchronization overhead on the critical path, best-effort error tracking is enough here.*/
	private Throwable flusherException;
	private volatile Throwable volatileFlusherException;
	private int volatileFlusherExceptionCheckSkipCount;
	private static final int VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT = 100;

	RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, long timeout, String taskName) {
		this.targetPartition = writer;
		this.numberOfChannels = writer.getNumberOfSubpartitions();
		this.channelSelector = checkNotNull(channelSelector);
		this.channelSelector.setup(numberOfChannels);
		this.isBroadcastSelector = channelSelector.isBroadcast();

		this.serializer = new DataOutputSerializer(64 * 1024);

		checkArgument(timeout >= -1);
		this.flushAlways = (timeout == 0);
		if (timeout == -1 || timeout == 0) {
			outputFlusher = null;
		} else {
			String threadName = taskName == null ?
				DEFAULT_OUTPUT_FLUSH_THREAD_NAME :
				DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for " + taskName;

			outputFlusher = new OutputFlusher(threadName, timeout);
			outputFlusher.start();
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException {
		broadcastEvent(event, false);
	}

	public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
		targetPartition.broadcastEvent(event, isPriorityEvent);
		if (flushAlways) {
			flushAll();
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
		targetPartition.setMetricGroup(metrics);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return targetPartition.getAvailableFuture();
	}

	/**
	 * This is used to send regular records.
	 */
	public void emit(T record) throws IOException, InterruptedException {
		if (isBroadcastSelector) {
			broadcastEmit(record);
		} else {
			int targetChannel = channelSelector.selectChannel(record);
			emit(record, targetChannel);
		}
	}

	/**
	 * This is used to send LatencyMarks to a random target channel.
	 */
	public void randomEmit(T record) throws IOException, InterruptedException {
		int targetChannel = rng.nextInt(numberOfChannels);
		emit(record, targetChannel);
	}

	/**
	 * This is used to broadcast streaming Watermarks in-band with records.
	 */
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		checkErroneous();

		targetPartition.broadcastWrite(serializeRecord(record), isBroadcastSelector);
		// Make sure we don't hold onto the large intermediate serialization buffer
		// for too long and the serializer is cleared to serialize the next record
		serializer.pruneBuffer();

		if (flushAlways) {
			flushAll();
		}
	}

	protected void emit(T record, int targetChannel) throws IOException, InterruptedException {
		checkErroneous();

		targetPartition.writerRecord(serializeRecord(record), targetChannel, isBroadcastSelector);
		// Make sure we don't hold onto the large intermediate serialization buffer
		// for too long and the serializer is cleared to serialize the next record
		serializer.pruneBuffer();

		if (flushAlways) {
			flushTargetPartition(targetChannel);
		}
	}

	private ByteBuffer serializeRecord(T record) throws IOException {
		serializer.skipBytesToWrite(4);
		record.write(serializer);
		serializer.writeIntUnsafe(serializer.length() - 4, 0);

		return serializer.wrapAsByteBuffer();
	}

	/**
	 * Closes the writer. This stops the flushing thread (if there is one).
	 */
	public void close() {
		// make sure we terminate the thread in any case
		if (outputFlusher != null) {
			outputFlusher.terminate();
			try {
				outputFlusher.join();
			} catch (InterruptedException e) {
				// ignore on close
				// restore interrupt flag to fast exit further blocking calls
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * Notifies the writer that the output flusher thread encountered an exception.
	 *
	 * @param t The exception to report.
	 */
	private void notifyFlusherException(Throwable t) {
		if (flusherException == null) {
			LOG.error("An exception happened while flushing the outputs", t);
			flusherException = t;
			volatileFlusherException = t;
		}
	}

	protected void checkErroneous() throws IOException {
		// For performance reasons, we are not checking volatile field every single time.
		if (flusherException != null ||
				(volatileFlusherExceptionCheckSkipCount >= VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT && volatileFlusherException != null)) {
			throw new IOException("An exception happened while flushing the outputs", volatileFlusherException);
		}
		if (++volatileFlusherExceptionCheckSkipCount >= VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT) {
			volatileFlusherExceptionCheckSkipCount = 0;
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
	 *
	 * <p>The thread is daemonic, because it is only a utility thread.
	 */
	private class OutputFlusher extends Thread {

		private final long timeout;

		private volatile boolean running = true;

		OutputFlusher(String name, long timeout) {
			super(name);
			setDaemon(true);
			this.timeout = timeout;
		}

		public void terminate() {
			running = false;
			interrupt();
		}

		@Override
		public void run() {
			try {
				while (running) {
					try {
						Thread.sleep(timeout);
					} catch (InterruptedException e) {
						// propagate this if we are still running, because it should not happen
						// in that case
						if (running) {
							throw new Exception(e);
						}
					}

					// any errors here should let the thread come to a halt and be
					// recognized by the writer
					flushAll();
				}
			} catch (Throwable t) {
				notifyFlusherException(t);
			}
		}
	}

	@VisibleForTesting
	ResultPartitionWriter getTargetPartition() {
		return targetPartition;
	}
}

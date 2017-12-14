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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.util.Random;

import static org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;

/**
 * A record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flush()} after
 * all records have been written with {@link #emit(IOReadableWritable)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends IOReadableWritable> {

	protected final ResultPartitionWriter targetPartition;

	private final ChannelSelector<T> channelSelector;

	private final int numChannels;

	/** {@link RecordSerializer} per outgoing channel */
	private final RecordSerializer<T>[] serializers;

	private final Random RNG = new XORShiftRandom();

	private Counter numBytesOut = new SimpleCounter();

	public RecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>());
	}

	@SuppressWarnings("unchecked")
	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this.targetPartition = writer;
		this.channelSelector = channelSelector;

		this.numChannels = writer.getNumberOfSubpartitions();

		/**
		 * The runtime exposes a channel abstraction for the produced results
		 * (see {@link ChannelSelector}). Every channel has an independent
		 * serializer.
		 */
		this.serializers = new SpanningRecordSerializer[numChannels];
		for (int i = 0; i < numChannels; i++) {
			serializers[i] = new SpanningRecordSerializer<T>();
		}
	}

	public void emit(T record) throws IOException, InterruptedException {
		for (int targetChannel : channelSelector.selectChannels(record, numChannels)) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to broadcast Streaming Watermarks in-band with records. This ignores
	 * the {@link ChannelSelector}.
	 */
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to send LatencyMarks to a random target channel
	 */
	public void randomEmit(T record) throws IOException, InterruptedException {
		sendToTarget(record, RNG.nextInt(numChannels));
	}

	private void sendToTarget(T record, int targetChannel) throws IOException, InterruptedException {
		RecordSerializer<T> serializer = serializers[targetChannel];

		synchronized (serializer) {
			SerializationResult result = serializer.addRecord(record);

			while (result.isFullBuffer()) {
				Buffer buffer = serializer.getCurrentBuffer();

				if (buffer != null) {
					numBytesOut.inc(buffer.getSizeUnsafe());
					writeAndClearBuffer(buffer, targetChannel, serializer);

					// If this was a full record, we are done. Not breaking
					// out of the loop at this point will lead to another
					// buffer request before breaking out (that would not be
					// a problem per se, but it can lead to stalls in the
					// pipeline).
					if (result.isFullRecord()) {
						break;
					}
				} else {
					buffer = targetPartition.getBufferProvider().requestBufferBlocking();
					result = serializer.setNextBuffer(buffer);
				}
			}
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
		final Buffer eventBuffer = EventSerializer.toBuffer(event);
		try {
			for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
				RecordSerializer<T> serializer = serializers[targetChannel];

				synchronized (serializer) {
					Buffer buffer = serializer.getCurrentBuffer();
					if (buffer != null) {
						numBytesOut.inc(buffer.getSizeUnsafe());
						writeAndClearBuffer(buffer, targetChannel, serializer);
					} else if (serializer.hasData()) {
						// sanity check
						throw new IllegalStateException("No buffer, but serializer has buffered data.");
					}

					// retain the buffer so that it can be recycled by each channel of targetPartition
					eventBuffer.retain();
					targetPartition.writeBuffer(eventBuffer, targetChannel);
				}
			}
		} finally {
			// we do not need to further retain the eventBuffer
			// (it will be recycled after the last channel stops using it)
			eventBuffer.recycle();
		}
	}

	public void flush() throws IOException {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			RecordSerializer<T> serializer = serializers[targetChannel];

			synchronized (serializer) {
				try {
					Buffer buffer = serializer.getCurrentBuffer();

					if (buffer != null) {
						numBytesOut.inc(buffer.getSizeUnsafe());
						targetPartition.writeBuffer(buffer, targetChannel);
					}
				} finally {
					serializer.clear();
				}
			}
		}
	}

	public void clearBuffers() {
		for (RecordSerializer<?> serializer : serializers) {
			synchronized (serializer) {
				try {
					Buffer buffer = serializer.getCurrentBuffer();

					if (buffer != null) {
						buffer.recycle();
					}
				}
				finally {
					serializer.clear();
				}
			}
		}
	}

	/**
	 * Sets the metric group for this RecordWriter.
	 * @param metrics
     */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		numBytesOut = metrics.getNumBytesOutCounter();
	}

	/**
	 * Writes the buffer to the {@link ResultPartitionWriter} and removes the
	 * buffer from the serializer state.
	 *
	 * Needs to be synchronized on the serializer!
	 */
	private void writeAndClearBuffer(
			Buffer buffer,
			int targetChannel,
			RecordSerializer<T> serializer) throws IOException {

		try {
			targetPartition.writeBuffer(buffer, targetChannel);
		}
		finally {
			serializer.clearCurrentBuffer();
		}
	}

}

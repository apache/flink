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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.api.reader.AbstractReader;
import org.apache.flink.runtime.io.network.api.reader.ReaderBase;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.CheckpointBarrier;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}.
 *
 * <p>
 * This also keeps track of {@link org.apache.flink.streaming.api.watermark.Watermark} events and forwards them to event subscribers
 * once the {@link org.apache.flink.streaming.api.watermark.Watermark} from all inputs advances.
 *
 * @param <T1> The type of the records that arrive on the first input
 * @param <T2> The type of the records that arrive on the second input
 */
public class CoStreamingRecordReader<T1 extends IOReadableWritable, T2 extends IOReadableWritable> extends AbstractReader implements ReaderBase, StreamingReader {

	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(CoStreamingRecordReader.class);

	private final RecordDeserializer[] recordDeserializers;

	private RecordDeserializer currentRecordDeserializer;

	private boolean isFinished;

	private final BarrierBuffer barrierBuffer;

	private long[] watermarks1;
	private long lastEmittedWatermark1;

	private long[] watermarks2;
	private long lastEmittedWatermark2;

	private int numInputChannels1;
	private int numInputChannels2;

	private int currentInputIndex = -1;

	@SuppressWarnings("unchecked")
	public CoStreamingRecordReader(Collection<InputGate> inputGates1, Collection<InputGate> inputGates2) {
		super(InputGateUtil.createInputGate(inputGates1, inputGates2));

		barrierBuffer = new BarrierBuffer(inputGate, this);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate
				.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer();
		}

		// determine which unioned channels belong to input 1 and which belong to input 2
		numInputChannels1 = 0;
		for (InputGate gate: inputGates1) {
			numInputChannels1 += gate.getNumberOfInputChannels();
		}
		numInputChannels2 = inputGate.getNumberOfInputChannels() - numInputChannels1;

		watermarks1 = new long[numInputChannels1];
		for (int i = 0; i < numInputChannels1; i++) {
			watermarks1[i] = Long.MIN_VALUE;
		}
		lastEmittedWatermark1 = Long.MIN_VALUE;

		watermarks2 = new long[numInputChannels2];
		for (int i = 0; i < numInputChannels2; i++) {
			watermarks2[i] = Long.MIN_VALUE;
		}
		lastEmittedWatermark2 = Long.MIN_VALUE;
	}

	@SuppressWarnings("unchecked")
	public int getNextRecord(T1 target1, T2 target2) throws IOException, InterruptedException {
		if (isFinished) {
			return 0;
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result;
				if (currentInputIndex == 1) {
					result = currentRecordDeserializer.getNextRecord(target1);
				} else if (currentInputIndex == 2) {
					result = currentRecordDeserializer.getNextRecord(target2);
				} else {
					throw new IllegalStateException("Wrong input index: " + currentInputIndex);
				}

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					return currentInputIndex;
				}
			}

			final BufferOrEvent bufferOrEvent = barrierBuffer.getNextNonBlocked();

			if (bufferOrEvent.isBuffer()) {
				currentRecordDeserializer = recordDeserializers[bufferOrEvent.getChannelIndex()];
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				if (bufferOrEvent.getChannelIndex() < numInputChannels1) {
					currentInputIndex = 1;
				} else {
					currentInputIndex = 2;
				}
			} else {
				// Event received
				final AbstractEvent event = bufferOrEvent.getEvent();

				if (event instanceof CheckpointBarrier) {
					barrierBuffer.processBarrier(bufferOrEvent);
				} else if (event instanceof Watermark) {
					Watermark mark = (Watermark) event;
					int inputIndex = bufferOrEvent.getChannelIndex();
					handleWatermark(mark, inputIndex);
				} else {
					if (handleEvent(event)) {
						if (inputGate.isFinished()) {
							if (!barrierBuffer.isEmpty()) {
								throw new RuntimeException("BarrierBuffer should be empty at this point");
							}
							isFinished = true;
							return 0;
						} else if (hasReachedEndOfSuperstep()) {
							return 0;
						} // else: More data is coming...
					}
				}
			}
		}
	}

	private void handleWatermark(Watermark mark, int channelIndex) throws IOException {
		if (channelIndex < numInputChannels1) {
			int logicalInputChannel = channelIndex;
			long watermarkMillis = mark.getTimestamp().getMillis();
			if (watermarkMillis > watermarks1[logicalInputChannel]) {
				watermarks1[logicalInputChannel] = watermarkMillis;
				long newMinWatermark = Long.MAX_VALUE;
				for (int i = 0; i < watermarks1.length; i++) {
					if (watermarks1[i] < newMinWatermark) {
						newMinWatermark = watermarks1[i];
					}
				}
				if (newMinWatermark > lastEmittedWatermark1) {
					lastEmittedWatermark1 = newMinWatermark;
					handleEvent(new Watermark(new Instant(lastEmittedWatermark1), 1));
				}
			}
		} else {
			int logicalInputChannel = channelIndex - numInputChannels1;
			long watermarkMillis = mark.getTimestamp().getMillis();
			if (watermarkMillis > watermarks2[logicalInputChannel]) {
				watermarks2[logicalInputChannel] = watermarkMillis;
				long newMinWatermark = Long.MAX_VALUE;
				for (int i = 0; i < watermarks2.length; i++) {
					if (watermarks2[i] < newMinWatermark) {
						newMinWatermark = watermarks2[i];
					}
				}
				if (newMinWatermark > lastEmittedWatermark2) {
					lastEmittedWatermark2 = newMinWatermark;
					handleEvent(new Watermark(new Instant(lastEmittedWatermark2), 2));
				}
			}
		}

	}

	public void clearBuffers() {
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}
	}

	public void cleanup() throws IOException {
		barrierBuffer.cleanup();
	}
}

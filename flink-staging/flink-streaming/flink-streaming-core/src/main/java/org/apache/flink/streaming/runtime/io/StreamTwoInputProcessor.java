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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.api.reader.AbstractReader;
import org.apache.flink.runtime.io.network.api.reader.ReaderBase;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.CheckpointBarrier;
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
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
public class StreamTwoInputProcessor<IN1, IN2> extends AbstractReader implements ReaderBase, StreamingReader {

	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(StreamTwoInputProcessor.class);

	private final RecordDeserializer<DeserializationDelegate<Object>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<Object>> currentRecordDeserializer;

	// We need to keep track of the channel from which a buffer came, so that we can
	// appropriately map the watermarks to input channels
	int currentChannel = -1;

	private boolean isFinished;

	private final BarrierBuffer barrierBuffer;

	private long[] watermarks1;
	private long lastEmittedWatermark1;

	private long[] watermarks2;
	private long lastEmittedWatermark2;

	private int numInputChannels1;
	private int numInputChannels2;

	private DeserializationDelegate<Object> deserializationDelegate1;
	private DeserializationDelegate<Object> deserializationDelegate2;

	@SuppressWarnings("unchecked")
	public StreamTwoInputProcessor(
			Collection<InputGate> inputGates1,
			Collection<InputGate> inputGates2,
			TypeSerializer<IN1> inputSerializer1,
			TypeSerializer<IN2> inputSerializer2,
			boolean enableWatermarkMultiplexing) {
		super(InputGateUtil.createInputGate(inputGates1, inputGates2));

		barrierBuffer = new BarrierBuffer(inputGate, this);

		StreamRecordSerializer<IN1> inputRecordSerializer1;
		if (enableWatermarkMultiplexing) {
			inputRecordSerializer1 = new MultiplexingStreamRecordSerializer<IN1>(inputSerializer1);
		} else {
			inputRecordSerializer1 = new StreamRecordSerializer<IN1>(inputSerializer1);
		}
		this.deserializationDelegate1 = new NonReusingDeserializationDelegate(inputRecordSerializer1);

		StreamRecordSerializer<IN2> inputRecordSerializer2;
		if (enableWatermarkMultiplexing) {
			inputRecordSerializer2 = new MultiplexingStreamRecordSerializer<IN2>(inputSerializer2);
		} else {
			inputRecordSerializer2 = new StreamRecordSerializer<IN2>(inputSerializer2);
		}
		this.deserializationDelegate2 = new NonReusingDeserializationDelegate(inputRecordSerializer2);

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
	public boolean processInput(TwoInputStreamOperator<IN1, IN2, ?> streamOperator) throws Exception {
		if (isFinished) {
			return false;
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result;
				if (currentChannel < numInputChannels1) {
					result = currentRecordDeserializer.getNextRecord(deserializationDelegate1);
				} else {
					result = currentRecordDeserializer.getNextRecord(deserializationDelegate2);
				}

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					if (currentChannel < numInputChannels1) {
						Object recordOrWatermark = deserializationDelegate1.getInstance();
						if (recordOrWatermark instanceof Watermark) {
							handleWatermark(streamOperator, (Watermark) recordOrWatermark, currentChannel);
							continue;
						} else {
							streamOperator.processElement1((StreamRecord<IN1>) deserializationDelegate1.getInstance());
							return true;

						}
					} else {
						Object recordOrWatermark = deserializationDelegate2.getInstance();
						if (recordOrWatermark instanceof Watermark) {
							handleWatermark(streamOperator, (Watermark) recordOrWatermark, currentChannel);
							continue;
						} else {
							streamOperator.processElement2((StreamRecord<IN2>) deserializationDelegate2.getInstance());
							return true;
						}
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierBuffer.getNextNonBlocked();

			if (bufferOrEvent.isBuffer()) {
				currentChannel = bufferOrEvent.getChannelIndex();
				currentRecordDeserializer = recordDeserializers[currentChannel];
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());

			} else {
				// Event received
				final AbstractEvent event = bufferOrEvent.getEvent();

				if (event instanceof CheckpointBarrier) {
					barrierBuffer.processBarrier(bufferOrEvent);
				} else {
					if (handleEvent(event)) {
						if (inputGate.isFinished()) {
							if (!barrierBuffer.isEmpty()) {
								throw new RuntimeException("BarrierBuffer should be empty at this point");
							}
							isFinished = true;
							return false;
						} else if (hasReachedEndOfSuperstep()) {
							return false;
						} // else: More data is coming...
					}
				}
			}
		}
	}

	private void handleWatermark(TwoInputStreamOperator<IN1, IN2, ?> operator, Watermark mark, int channelIndex) throws Exception {
		if (channelIndex < numInputChannels1) {
			long watermarkMillis = mark.getTimestamp();
			if (watermarkMillis > watermarks1[channelIndex]) {
				watermarks1[channelIndex] = watermarkMillis;
				long newMinWatermark = Long.MAX_VALUE;
				for (long aWatermarks1 : watermarks1) {
					if (aWatermarks1 < newMinWatermark) {
						newMinWatermark = aWatermarks1;
					}
				}
				if (newMinWatermark > lastEmittedWatermark1) {
					lastEmittedWatermark1 = newMinWatermark;
					operator.processWatermark1(new Watermark(lastEmittedWatermark1));
				}
			}
		} else {
			channelIndex = channelIndex - numInputChannels1;
			long watermarkMillis = mark.getTimestamp();
			if (watermarkMillis > watermarks2[channelIndex]) {
				watermarks2[channelIndex] = watermarkMillis;
				long newMinWatermark = Long.MAX_VALUE;
				for (long aWatermarks2 : watermarks2) {
					if (aWatermarks2 < newMinWatermark) {
						newMinWatermark = aWatermarks2;
					}
				}
				if (newMinWatermark > lastEmittedWatermark2) {
					lastEmittedWatermark2 = newMinWatermark;
					operator.processWatermark2(new Watermark(lastEmittedWatermark2));
				}
			}
		}

	}

	@Override
	public void setReporter(AccumulatorRegistry.Reporter reporter) {
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			deserializer.setReporter(reporter);
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

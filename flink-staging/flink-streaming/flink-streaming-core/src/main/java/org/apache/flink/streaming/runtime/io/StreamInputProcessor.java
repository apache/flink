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

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
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
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.CheckpointBarrier;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>This also keeps track of {@link Watermark} events and forwards them to event subscribers
 * once the {@link Watermark} from all inputs advances.</p>
 * 
 * @param <IN> The type of the record that can be read with this record reader.
 */
public class StreamInputProcessor<IN> extends AbstractReader implements ReaderBase, StreamingReader {

	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(StreamInputProcessor.class);

	private final RecordDeserializer<DeserializationDelegate<Object>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<Object>> currentRecordDeserializer;

	// We need to keep track of the channel from which a buffer came, so that we can
	// appropriately map the watermarks to input channels
	private int currentChannel = -1;

	private boolean isFinished;

	private final CheckpointBarrierHandler barrierHandler;

	private final long[] watermarks;
	private long lastEmittedWatermark;

	private final DeserializationDelegate<Object> deserializationDelegate;

	@SuppressWarnings("unchecked")
	public StreamInputProcessor(InputGate[] inputGates, TypeSerializer<IN> inputSerializer,
								EventListener<CheckpointBarrier> checkpointListener,
								IOManager ioManager,
								boolean enableWatermarkMultiplexing) throws IOException {
		
		super(InputGateUtil.createInputGate(inputGates));

		this.barrierHandler = new BarrierBuffer(inputGate, ioManager);
		if (checkpointListener != null) {
			this.barrierHandler.registerCheckpointEventHandler(checkpointListener);
		}
		
		if (enableWatermarkMultiplexing) {
			MultiplexingStreamRecordSerializer<IN> ser = new MultiplexingStreamRecordSerializer<IN>(inputSerializer);
			this.deserializationDelegate = new NonReusingDeserializationDelegate<Object>(ser);
		} else {
			StreamRecordSerializer<IN> ser = new StreamRecordSerializer<IN>(inputSerializer);
			this.deserializationDelegate = (NonReusingDeserializationDelegate<Object>)
					(NonReusingDeserializationDelegate<?>) new NonReusingDeserializationDelegate<StreamRecord<IN>>(ser);
		}
		
		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];
		
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<DeserializationDelegate<Object>>();
		}

		watermarks = new long[inputGate.getNumberOfInputChannels()];
		for (int i = 0; i < inputGate.getNumberOfInputChannels(); i++) {
			watermarks[i] = Long.MIN_VALUE;
		}
		lastEmittedWatermark = Long.MIN_VALUE;
	}
	
	
	public boolean processInput(OneInputStreamOperator<IN, ?> streamOperator) throws Exception {
		if (isFinished) {
			return false;
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					Object recordOrWatermark = deserializationDelegate.getInstance();

					if (recordOrWatermark instanceof Watermark) {
						Watermark mark = (Watermark) recordOrWatermark;
						long watermarkMillis = mark.getTimestamp();
						if (watermarkMillis > watermarks[currentChannel]) {
							watermarks[currentChannel] = watermarkMillis;
							long newMinWatermark = Long.MAX_VALUE;
							for (long watermark : watermarks) {
								if (watermark < newMinWatermark) {
									newMinWatermark = watermark;
								}
							}
							if (newMinWatermark > lastEmittedWatermark) {
								lastEmittedWatermark = newMinWatermark;
								streamOperator.processWatermark(new Watermark(lastEmittedWatermark));
							}
						}
						continue;
					}
					else {
						// now we can do the actual processing
						@SuppressWarnings("unchecked")
						StreamRecord<IN> record = (StreamRecord<IN>) deserializationDelegate.getInstance();
						StreamingRuntimeContext ctx = streamOperator.getRuntimeContext();
						if (ctx != null) {
							ctx.setNextInput(record);
						}
						streamOperator.processElement(record);
						return true;
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				}
				else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					handleEvent(event);
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
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

	@Override
	public void cleanup() throws IOException {
		barrierHandler.cleanup();
	}
}

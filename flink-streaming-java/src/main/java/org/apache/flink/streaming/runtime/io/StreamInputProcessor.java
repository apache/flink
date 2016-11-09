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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>
 * This also keeps track of {@link Watermark} events and forwards them to event subscribers
 * once the {@link Watermark} from all inputs advances.
 *
 * <p>
 * Forwarding elements or watermarks must be protected by synchronizing on the given lock
 * object. This ensures that we don't call methods on a {@link OneInputStreamOperator} concurrently
 * with the timer callback or other things.
 * 
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
public class StreamInputProcessor<IN> {

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private final CheckpointBarrierHandler barrierHandler;

	// We need to keep track of the channel from which a buffer came, so that we can
	// appropriately map the watermarks to input channels
	private int currentChannel = -1;

	private boolean isFinished;

	private final long[] watermarks;
	private long lastEmittedWatermark;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private Counter numRecordsIn;

	@SuppressWarnings("unchecked")
	public StreamInputProcessor(
			InputGate[] inputGates,
			TypeSerializer<IN> inputSerializer,
			StatefulTask checkpointedTask,
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			Configuration taskManagerConfig) throws IOException {

		InputGate inputGate = InputGateUtil.createInputGate(inputGates);

		if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
			long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
			if (!(maxAlign == -1 || maxAlign > 0)) {
				throw new IllegalConfigurationException(
						TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
						+ " must be positive or -1 (infinite)");
			}
			this.barrierHandler = new BarrierBuffer(inputGate, ioManager, maxAlign);
		}
		else if (checkpointMode == CheckpointingMode.AT_LEAST_ONCE) {
			this.barrierHandler = new BarrierTracker(inputGate);
		}
		else {
			throw new IllegalArgumentException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}
		
		if (checkpointedTask != null) {
			this.barrierHandler.registerCheckpointEventHandler(checkpointedTask);
		}
		
		StreamElementSerializer<IN> ser = new StreamElementSerializer<>(inputSerializer);
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(ser);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];
		
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
					ioManager.getSpillingDirectoriesPaths());
		}

		watermarks = new long[inputGate.getNumberOfInputChannels()];
		for (int i = 0; i < inputGate.getNumberOfInputChannels(); i++) {
			watermarks[i] = Long.MIN_VALUE;
		}
		lastEmittedWatermark = Long.MIN_VALUE;
	}

	@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
	public boolean processInput(OneInputStreamOperator<IN, ?> streamOperator, final Object lock) throws Exception {
		if (isFinished) {
			return false;
		}
		if (numRecordsIn == null) {
			numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrMark = deserializationDelegate.getInstance();

					if (recordOrMark.isWatermark()) {
						long watermarkMillis = recordOrMark.asWatermark().getTimestamp();
						if (watermarkMillis > watermarks[currentChannel]) {
							watermarks[currentChannel] = watermarkMillis;
							long newMinWatermark = Long.MAX_VALUE;
							for (long watermark: watermarks) {
								newMinWatermark = Math.min(watermark, newMinWatermark);
							}
							if (newMinWatermark > lastEmittedWatermark) {
								lastEmittedWatermark = newMinWatermark;
								synchronized (lock) {
									streamOperator.processWatermark(new Watermark(lastEmittedWatermark));
								}
							}
						}
						continue;
					} else if(recordOrMark.isLatencyMarker()) {
						// handle latency marker
						synchronized (lock) {
							streamOperator.processLatencyMarker(recordOrMark.asLatencyMarker());
						}
						continue;
					} else {
						// now we can do the actual processing
						StreamRecord<IN> record = recordOrMark.asRecord();
						synchronized (lock) {
							numRecordsIn.inc();
							streamOperator.setKeyContextElement1(record);
							streamOperator.processElement(record);
						}
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
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
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

	/**
	 * Sets the metric group for this StreamInputProcessor.
	 * 
	 * @param metrics metric group
	 */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		metrics.gauge("currentLowWatermark", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return lastEmittedWatermark;
			}
		});

		metrics.gauge("checkpointAlignmentTime", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return barrierHandler.getAlignmentDurationNanos();
			}
		});
	}
	
	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}
		
		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}
}

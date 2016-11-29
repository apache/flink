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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
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
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}.
 *
 * <p>
 * This also keeps track of {@link org.apache.flink.streaming.api.watermark.Watermark} events and forwards them to event subscribers
 * once the {@link org.apache.flink.streaming.api.watermark.Watermark} from all inputs advances.
 *
 * <p>
 * Forwarding elements or watermarks must be protected by synchronizing on the given lock
 * object. This ensures that we don't call methods on a {@link TwoInputStreamOperator} concurrently
 * with the timer callback or other things.
 *
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
@Internal
public class StreamTwoInputProcessor<IN1, IN2> {

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	// We need to keep track of the channel from which a buffer came, so that we can
	// appropriately map the watermarks to input channels
	private int currentChannel = -1;

	private boolean isFinished;

	private final CheckpointBarrierHandler barrierHandler;

	private final long[] watermarks1;
	private long lastEmittedWatermark1;

	private final long[] watermarks2;
	private long lastEmittedWatermark2;

	private final int numInputChannels1;

	private final DeserializationDelegate<StreamElement> deserializationDelegate1;
	private final DeserializationDelegate<StreamElement> deserializationDelegate2;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public StreamTwoInputProcessor(
			Collection<InputGate> inputGates1,
			Collection<InputGate> inputGates2,
			TypeSerializer<IN1> inputSerializer1,
			TypeSerializer<IN2> inputSerializer2,
			StatefulTask checkpointedTask,
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			Configuration taskManagerConfig) throws IOException {

		final InputGate inputGate = InputGateUtil.createInputGate(inputGates1, inputGates2);

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
			throw new IllegalArgumentException("Unrecognized CheckpointingMode: " + checkpointMode);
		}
		
		if (checkpointedTask != null) {
			this.barrierHandler.registerCheckpointEventHandler(checkpointedTask);
		}
		
		StreamElementSerializer<IN1> ser1 = new StreamElementSerializer<>(inputSerializer1);
		this.deserializationDelegate1 = new NonReusingDeserializationDelegate<>(ser1);

		StreamElementSerializer<IN2> ser2 = new StreamElementSerializer<>(inputSerializer2);
		this.deserializationDelegate2 = new NonReusingDeserializationDelegate<>(ser2);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];
		
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
					ioManager.getSpillingDirectoriesPaths());
		}

		// determine which unioned channels belong to input 1 and which belong to input 2
		int numInputChannels1 = 0;
		for (InputGate gate: inputGates1) {
			numInputChannels1 += gate.getNumberOfInputChannels();
		}
		
		this.numInputChannels1 = numInputChannels1;
		int numInputChannels2 = inputGate.getNumberOfInputChannels() - numInputChannels1;

		watermarks1 = new long[numInputChannels1];
		Arrays.fill(watermarks1, Long.MIN_VALUE);
		lastEmittedWatermark1 = Long.MIN_VALUE;

		watermarks2 = new long[numInputChannels2];
		Arrays.fill(watermarks2, Long.MIN_VALUE);
		lastEmittedWatermark2 = Long.MIN_VALUE;
	}

	@SuppressWarnings("unchecked")
	public boolean processInput(TwoInputStreamOperator<IN1, IN2, ?> streamOperator, Object lock) throws Exception {
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
						StreamElement recordOrWatermark = deserializationDelegate1.getInstance();
						if (recordOrWatermark.isWatermark()) {
							handleWatermark(streamOperator, recordOrWatermark.asWatermark(), currentChannel, lock);
							continue;
						}
						else if (recordOrWatermark.isLatencyMarker()) {
							synchronized (lock) {
								streamOperator.processLatencyMarker1(recordOrWatermark.asLatencyMarker());
							}
							continue;
						}
						else {
							synchronized (lock) {
								streamOperator.setKeyContextElement1(recordOrWatermark.<IN1>asRecord());
								streamOperator.processElement1(recordOrWatermark.<IN1>asRecord());
							}
							return true;

						}
					}
					else {
						StreamElement recordOrWatermark = deserializationDelegate2.getInstance();
						if (recordOrWatermark.isWatermark()) {
							handleWatermark(streamOperator, recordOrWatermark.asWatermark(), currentChannel, lock);
							continue;
						}
						else if (recordOrWatermark.isLatencyMarker()) {
							synchronized (lock) {
								streamOperator.processLatencyMarker2(recordOrWatermark.asLatencyMarker());
							}
							continue;
						}
						else {
							synchronized (lock) {
								streamOperator.setKeyContextElement2(recordOrWatermark.<IN2>asRecord());
								streamOperator.processElement2(recordOrWatermark.<IN2>asRecord());
							}
							return true;
						}
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {

				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
	
				} else {
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

	private void handleWatermark(TwoInputStreamOperator<IN1, IN2, ?> operator, Watermark mark, int channelIndex, Object lock) throws Exception {
		if (channelIndex < numInputChannels1) {
			long watermarkMillis = mark.getTimestamp();
			if (watermarkMillis > watermarks1[channelIndex]) {
				watermarks1[channelIndex] = watermarkMillis;
				long newMinWatermark = Long.MAX_VALUE;
				for (long wm : watermarks1) {
					newMinWatermark = Math.min(wm, newMinWatermark);
				}
				if (newMinWatermark > lastEmittedWatermark1) {
					lastEmittedWatermark1 = newMinWatermark;
					synchronized (lock) {
						operator.processWatermark1(new Watermark(lastEmittedWatermark1));
					}
				}
			}
		} else {
			channelIndex = channelIndex - numInputChannels1;
			long watermarkMillis = mark.getTimestamp();
			if (watermarkMillis > watermarks2[channelIndex]) {
				watermarks2[channelIndex] = watermarkMillis;
				long newMinWatermark = Long.MAX_VALUE;
				for (long wm : watermarks2) {
					newMinWatermark = Math.min(wm, newMinWatermark);
				}
				if (newMinWatermark > lastEmittedWatermark2) {
					lastEmittedWatermark2 = newMinWatermark;
					synchronized (lock) {
						operator.processWatermark2(new Watermark(lastEmittedWatermark2));
					}
				}
			}
		}
	}

	/**
	 * Sets the metric group for this StreamTwoInputProcessor.
	 *
	 * @param metrics metric group
	 */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		metrics.gauge("currentLowWatermark", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return Math.min(lastEmittedWatermark1, lastEmittedWatermark2);
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

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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}.
 *
 * <p>
 * This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and {@link StreamStatus} events,
 * and forwards watermarks to event subscribers once the {@link StatusWatermarkValve} determines the watermarks from
 * all inputs has advanced, or changes the task's {@link StreamStatus} once status change is toggled.
 *
 * <p>
 * Forwarding elements, watermarks, or status status elements must be protected by synchronizing on the given lock
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

	private final DeserializationDelegate<StreamElement> deserializationDelegate1;
	private final DeserializationDelegate<StreamElement> deserializationDelegate2;

	private final CheckpointBarrierHandler barrierHandler;

	private final Object lock;

	// ---------------- Status and Watermark Valves ------------------

	/**
	 * Stream status for the two inputs. We need to keep track for determining when
	 * to forward stream status changes downstream.
	 */
	private StreamStatus firstStatus;
	private StreamStatus secondStatus;

	/**
	 * Valves that control how watermarks and stream statuses from the 2 inputs are forwarded.
	 */
	private StatusWatermarkValve statusWatermarkValve1;
	private StatusWatermarkValve statusWatermarkValve2;

	/** Number of input channels the valves need to handle. */
	private final int numInputChannels1;
	private final int numInputChannels2;

	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to the correct channel index of the correct valve.
	 */
	private int currentChannel = -1;

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final TwoInputStreamOperator<IN1, IN2, ?> streamOperator;

	// ---------------- Metrics ------------------

	private long lastEmittedWatermark1;
	private long lastEmittedWatermark2;

	private boolean isFinished;

	@SuppressWarnings("unchecked")
	public StreamTwoInputProcessor(
			Collection<InputGate> inputGates1,
			Collection<InputGate> inputGates2,
			TypeSerializer<IN1> inputSerializer1,
			TypeSerializer<IN2> inputSerializer2,
			StatefulTask checkpointedTask,
			CheckpointingMode checkpointMode,
			Object lock,
			IOManager ioManager,
			Configuration taskManagerConfig,
			StreamStatusMaintainer streamStatusMaintainer,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator) throws IOException {

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

		this.lock = checkNotNull(lock);
		
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
		this.numInputChannels2 = inputGate.getNumberOfInputChannels() - numInputChannels1;

		this.lastEmittedWatermark1 = Long.MIN_VALUE;
		this.lastEmittedWatermark2 = Long.MIN_VALUE;

		this.firstStatus = StreamStatus.ACTIVE;
		this.secondStatus = StreamStatus.ACTIVE;

		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.streamOperator = checkNotNull(streamOperator);

		this.statusWatermarkValve1 = new StatusWatermarkValve(numInputChannels1, new ForwardingValveOutputHandler1(streamOperator, lock));
		this.statusWatermarkValve2 = new StatusWatermarkValve(numInputChannels2, new ForwardingValveOutputHandler2(streamOperator, lock));

	}

	public boolean processInput() throws Exception {
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
							statusWatermarkValve1.inputWatermark(recordOrWatermark.asWatermark(), currentChannel);
							continue;
						}
						else if (recordOrWatermark.isStreamStatus()) {
							statusWatermarkValve1.inputStreamStatus(recordOrWatermark.asStreamStatus(), currentChannel);
							continue;
						}
						else if (recordOrWatermark.isLatencyMarker()) {
							synchronized (lock) {
								streamOperator.processLatencyMarker1(recordOrWatermark.asLatencyMarker());
							}
							continue;
						}
						else {
							StreamRecord<IN1> record = recordOrWatermark.asRecord();
							synchronized (lock) {
								streamOperator.setKeyContextElement1(record);
								streamOperator.processElement1(record);
							}
							return true;

						}
					}
					else {
						StreamElement recordOrWatermark = deserializationDelegate2.getInstance();
						if (recordOrWatermark.isWatermark()) {
							statusWatermarkValve2.inputWatermark(recordOrWatermark.asWatermark(), currentChannel - numInputChannels1);
							continue;
						}
						else if (recordOrWatermark.isStreamStatus()) {
							statusWatermarkValve2.inputStreamStatus(recordOrWatermark.asStreamStatus(), currentChannel - numInputChannels1);
							continue;
						}
						else if (recordOrWatermark.isLatencyMarker()) {
							synchronized (lock) {
								streamOperator.processLatencyMarker2(recordOrWatermark.asLatencyMarker());
							}
							continue;
						}
						else {
							StreamRecord<IN2> record = recordOrWatermark.asRecord();
							synchronized (lock) {
								streamOperator.setKeyContextElement2(record);
								streamOperator.processElement2(record);
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

	private class ForwardingValveOutputHandler1 implements StatusWatermarkValve.ValveOutputHandler {
		private final TwoInputStreamOperator<IN1, IN2, ?> operator;
		private final Object lock;

		private ForwardingValveOutputHandler1(final TwoInputStreamOperator<IN1, IN2, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					lastEmittedWatermark1 = watermark.getTimestamp();
					operator.processWatermark1(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					firstStatus = streamStatus;

					// check if we need to toggle the task's stream status
					if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
						if (streamStatus.isActive()) {
							// we're no longer idle if at least one input has become active
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
						} else if (secondStatus.isIdle()) {
							// we're idle once both inputs are idle
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
						}
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}

	private class ForwardingValveOutputHandler2 implements StatusWatermarkValve.ValveOutputHandler {
		private final TwoInputStreamOperator<IN1, IN2, ?> operator;
		private final Object lock;

		private ForwardingValveOutputHandler2(final TwoInputStreamOperator<IN1, IN2, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					lastEmittedWatermark2 = watermark.getTimestamp();
					operator.processWatermark2(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					secondStatus = streamStatus;

					// check if we need to toggle the task's stream status
					if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
						if (streamStatus.isActive()) {
							// we're no longer idle if at least one input has become active
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
						} else if (firstStatus.isIdle()) {
							// we're idle once both inputs are idle
							streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
						}
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}
}

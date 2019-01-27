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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SerializerManagerUtility;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.SumAndCount;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.plugable.ReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards watermarks to event subscribers once the
 * {@link StatusWatermarkValve} determines the watermarks from all inputs has advanced, or changes
 * the task's {@link StreamStatus} once status change is toggled.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link TwoInputStreamOperator} concurrently with the timer callback or other things.
 *
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
@Internal
public class StreamTwoInputProcessor<IN1, IN2> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTwoInputProcessor.class);

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializerOfChannels;

	private RecordDeserializer<DeserializationDelegate<StreamElement>>[] currentRecordDeserializerOfInputs;

	private final DeserializationDelegate<StreamElement>[] deserializationDelegateOfInputs;

	private final InputGate[] inputs;

	private final SelectedReadingBarrierHandler barrierHandler;

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
	private final StatusWatermarkValve[] statusWatermarkValveOfInputs;

	/** Number of input channels the valves need to handle. */
	private final int[] numChannelsOfInputs;

	private final BitSet[] eopChannelsOfInputs;

	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to the correct channel index of the correct valve.
	 */
	private int[] currentChannelOfInputs;

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final TwoInputStreamOperator<IN1, IN2, ?> streamOperator;

	private TwoInputSelection inputSelection;
	private boolean[] isEndInputs;

	private int lastReadingInputIndex = 0;

	// ---------------- Metrics ------------------

	private final WatermarkGauge[] watermarkGaugeOfInputs;

	private Counter numRecordsIn;

	private boolean isFinished;

	private boolean enableTracingMetrics;
	private int tracingMetricsInterval;

	private SumAndCount taskLatency;

	private SumAndCount waitInput;

	private long lastProcessedTime = -1;

	private IN1 reusedObject1;
	private IN2 reusedObject2;

	@SuppressWarnings("unchecked")
	public StreamTwoInputProcessor(
			Collection<InputGate> inputGates1,
			Collection<InputGate> inputGates2,
			TypeSerializer<IN1> inputSerializer1,
			TypeSerializer<IN2> inputSerializer2,
			boolean isCheckpointingEnabled,
			TwoInputStreamTask<IN1, IN2, ?> checkpointedTask,
			CheckpointingMode checkpointMode,
			Object lock,
			IOManager ioManager,
			Configuration taskManagerConfig,
			StreamStatusMaintainer streamStatusMaintainer,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			TaskIOMetricGroup metrics,
			WatermarkGauge input1WatermarkGauge,
			WatermarkGauge input2WatermarkGauge,
			boolean objectReuse,
			boolean enableTracingMetrics,
			int tracingMetricsInterval) throws IOException {

		this.barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
			isCheckpointingEnabled, checkpointedTask, checkpointMode, ioManager, taskManagerConfig, inputGates1, inputGates2);

		checkState(barrierHandler.getSubInputGateCount() == 2);
		int numberOfInputs = barrierHandler.getSubInputGateCount();

		this.inputs = new InputGate[numberOfInputs];
		for (int i = 0; i < numberOfInputs; i++) {
			this.inputs[i] = barrierHandler.getSubInputGate(i);
		}

		this.lock = checkNotNull(lock);

		StreamElementSerializer<IN1> ser1 = new StreamElementSerializer<>(inputSerializer1);
		StreamElementSerializer<IN2> ser2 = new StreamElementSerializer<>(inputSerializer2);
		this.deserializationDelegateOfInputs = new DeserializationDelegate[] {
			objectReuse ?
				new ReusingDeserializationDelegate<>(ser1) :
				new NonReusingDeserializationDelegate<>(ser1),
			objectReuse ?
				new ReusingDeserializationDelegate<>(ser2) :
				new NonReusingDeserializationDelegate<>(ser2)
		};

		this.reusedObject1 = objectReuse ? inputSerializer1.createInstance() : null;
		this.reusedObject2 = objectReuse ? inputSerializer2.createInstance() : null;

		int numberOfAllInputChannels = barrierHandler.getNumberOfInputChannels();

		// Initialize one deserializer per input channel
		SerializerManagerUtility<DeserializationDelegate<StreamElement>> serializerManagerUtility =
			new SerializerManagerUtility<>(taskManagerConfig);
		this.recordDeserializerOfChannels = serializerManagerUtility.createRecordDeserializers(
			barrierHandler.getAllInputChannels(), ioManager.getSpillingDirectoriesPaths());

		// determine which unioned channels belong to input 1 and which belong to input 2
		this.numChannelsOfInputs = new int[numberOfInputs];
		for (int i = 0; i < numberOfInputs; i++) {
			this.numChannelsOfInputs[i] = inputs[i].getNumberOfInputChannels();
		}

		this.eopChannelsOfInputs = new BitSet[numberOfInputs];
		for (int i = 0; i < numberOfInputs; i++) {
			this.eopChannelsOfInputs[i] = new BitSet(this.numChannelsOfInputs[i]);
		}

		this.firstStatus = StreamStatus.ACTIVE;
		this.secondStatus = StreamStatus.ACTIVE;

		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.streamOperator = checkNotNull(streamOperator);

		this.statusWatermarkValveOfInputs = new StatusWatermarkValve[] {
			new StatusWatermarkValve(numChannelsOfInputs[0], new ForwardingValveOutputHandler1(streamOperator, lock)),
			new StatusWatermarkValve(numChannelsOfInputs[1], new ForwardingValveOutputHandler2(streamOperator, lock))
		};

		this.watermarkGaugeOfInputs = new WatermarkGauge[] {
			input1WatermarkGauge,
			input2WatermarkGauge
		};
		metrics.gauge("checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);

		this.currentRecordDeserializerOfInputs = new RecordDeserializer[numberOfInputs];
		this.currentChannelOfInputs = new int[]{-1, -1};

		this.inputSelection = streamOperator.firstInputSelection();
		this.isEndInputs = new boolean[]{false, false};

		this.enableTracingMetrics = enableTracingMetrics;
		this.tracingMetricsInterval = tracingMetricsInterval;
	}

	public boolean processInput() throws Exception {
		if (isFinished) {
			return false;
		}
		if (numRecordsIn == null) {
			try {
				numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				numRecordsIn = new SimpleCounter();
			}
		}
		if (enableTracingMetrics) {
			if (taskLatency == null) {
				taskLatency = new SumAndCount(MetricNames.TASK_LATENCY, streamOperator.getMetricGroup());
			}
			if (waitInput == null) {
				waitInput = new SumAndCount(MetricNames.IO_WAIT_INPUT, streamOperator.getMetricGroup());
			}
		}

		while (true) {
			// select an input
			int readingIndex;
			if (inputSelection == TwoInputSelection.ANY) {
				int flag = ((currentRecordDeserializerOfInputs[0] == null) ? 0 : 1)
					| ((currentRecordDeserializerOfInputs[1] == null) ? 0 : 2);

				if (flag == 3) {
					readingIndex = lastReadingInputIndex;
				} else if (flag > 0) {
					readingIndex = (flag == 1) ? 0 : 1;
				} else {
					readingIndex = 0;
				}
			} else {
				readingIndex = getInputIndexFromInputSelection(inputSelection);
			}

			// read buffer of selected input
			if (currentRecordDeserializerOfInputs[readingIndex] != null) {
				lastReadingInputIndex = readingIndex;

				if (readingIndex == 0 && reusedObject1 != null){
					deserializationDelegateOfInputs[readingIndex].setInstance(new StreamRecord(reusedObject1));
				} else if (readingIndex == 1 && reusedObject2 != null) {
					deserializationDelegateOfInputs[readingIndex].setInstance(new StreamRecord(reusedObject2));
				}
				DeserializationResult result = currentRecordDeserializerOfInputs[readingIndex]
					.getNextRecord(deserializationDelegateOfInputs[readingIndex]);

				if (result.isBufferConsumed()) {
					currentRecordDeserializerOfInputs[readingIndex].getCurrentBuffer().recycleBuffer();
					currentRecordDeserializerOfInputs[readingIndex] = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrWatermark = deserializationDelegateOfInputs[readingIndex].getInstance();
					if (recordOrWatermark.isWatermark()) {
						statusWatermarkValveOfInputs[readingIndex].inputWatermark(recordOrWatermark.asWatermark(),
							currentChannelOfInputs[readingIndex] - (readingIndex == 0 ? 0 : numChannelsOfInputs[0]));
						continue;
					} else if (recordOrWatermark.isStreamStatus()) {
						statusWatermarkValveOfInputs[readingIndex].inputStreamStatus(recordOrWatermark.asStreamStatus(),
							currentChannelOfInputs[readingIndex] - (readingIndex == 0 ? 0 : numChannelsOfInputs[0]));
						continue;
					} else if (recordOrWatermark.isLatencyMarker()) {
						synchronized (lock) {
							if (readingIndex == 0) {
								streamOperator.processLatencyMarker1(recordOrWatermark.asLatencyMarker());
							} else {
								streamOperator.processLatencyMarker2(recordOrWatermark.asLatencyMarker());
							}
						}
						continue;
					} else {
						if (readingIndex == 0) {
							reusedObject1 = ((StreamRecord<IN1>) recordOrWatermark).getValue();

							StreamRecord<IN1> record = recordOrWatermark.asRecord();
							// numRecordsIn counter is a SimpleCounter not a heavier SumCounter, so reuse it
							if (enableTracingMetrics && numRecordsIn.getCount() % tracingMetricsInterval == 0) {
								long start = System.nanoTime();
								waitInput.update(start - lastProcessedTime);
								synchronized (lock) {
									numRecordsIn.inc();
									streamOperator.setKeyContextElement1(record);
									inputSelection = streamOperator.processElement1(record);
									lastProcessedTime = System.nanoTime();
									taskLatency.update(lastProcessedTime - start);
								}
							} else {
								synchronized (lock) {
									numRecordsIn.inc();
									streamOperator.setKeyContextElement1(record);
									inputSelection = streamOperator.processElement1(record);
								}
							}
						} else {
							reusedObject2 = ((StreamRecord<IN2>) recordOrWatermark).getValue();

							StreamRecord<IN2> record = recordOrWatermark.asRecord();
							// numRecordsIn counter is a SimpleCounter not a heavier SumCounter, so reuse it
							if (enableTracingMetrics && numRecordsIn.getCount() % tracingMetricsInterval == 0) {
								long start = System.nanoTime();
								waitInput.update(start - lastProcessedTime);
								synchronized (lock) {
									numRecordsIn.inc();
									streamOperator.setKeyContextElement2(record);
									inputSelection = streamOperator.processElement2(record);
									lastProcessedTime = System.nanoTime();
									taskLatency.update(lastProcessedTime - start);
								}
							} else {
								synchronized (lock) {
									numRecordsIn.inc();
									streamOperator.setKeyContextElement2(record);
									inputSelection = streamOperator.processElement2(record);
								}
							}
						}

						return true;
					}
				}
			}

			// read InputGate(s) of selected input
			final BufferOrEvent bufferOrEvent;
			if (inputSelection == TwoInputSelection.ANY) {
				bufferOrEvent = barrierHandler.getNextNonBlocked();
			} else {
				if (isEndInputs[readingIndex]) {
					throw new IOException("Unexpected reading selection: " + inputSelection + ", because the input has finished.");
				}

				bufferOrEvent = barrierHandler.getNextNonBlocked(inputs[readingIndex]);
			}

			if (bufferOrEvent != null) {

				if (bufferOrEvent.isBuffer()) {
					int channelIndex = bufferOrEvent.getChannelIndex();

					int inputIndex = getInputIndexFromChannel(channelIndex);
					currentRecordDeserializerOfInputs[inputIndex] = recordDeserializerOfChannels[channelIndex];
					currentRecordDeserializerOfInputs[inputIndex].setNextBuffer(bufferOrEvent.getBuffer());

					currentChannelOfInputs[inputIndex] = channelIndex;
					lastReadingInputIndex = inputIndex;
				} else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}

					int channelIndex = bufferOrEvent.getChannelIndex();
					int inputIndex = getInputIndexFromChannel(channelIndex);
					eopChannelsOfInputs[inputIndex].set(channelIndex);
					if (eopChannelsOfInputs[inputIndex].cardinality() == numChannelsOfInputs[inputIndex]) {
						synchronized (lock) {
							if (inputIndex == 0) {
								streamOperator.endInput1();
							} else {
								streamOperator.endInput2();
							}

							isEndInputs[inputIndex] = true;
							inputSelection = TwoInputSelection.ANY;
						}
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

	private int getInputIndexFromChannel(int channelIndex) {
		return channelIndex < this.numChannelsOfInputs[0] ? 0 : 1;
	}

	private int getInputIndexFromInputSelection(TwoInputSelection inputSelection) {
		return inputSelection == TwoInputSelection.FIRST ?
			0 : (inputSelection == TwoInputSelection.SECOND ? 1 : -1);
	}

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializerOfChannels) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
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
					watermarkGaugeOfInputs[0].setCurrentWatermark(watermark.getTimestamp());
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
					watermarkGaugeOfInputs[1].setCurrentWatermark(watermark.getTimestamp());
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

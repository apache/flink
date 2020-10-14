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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.SortingDataInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.AbstractDataOutput;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.InputProcessorUtil;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamTask} for executing a {@link OneInputStreamOperator}.
 */
@Internal
public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	public OneInputStreamTask(Environment env) throws Exception {
		super(env);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link TimerService}. By default (and if
	 * null is passes for the time provider) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param env The task environment for this task.
	 * @param timeProvider Optionally, a specific time provider to use.
	 */
	@VisibleForTesting
	public OneInputStreamTask(
			Environment env,
			@Nullable TimerService timeProvider) throws Exception {
		super(env, timeProvider);
	}

	@Override
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();
		int numberOfInputs = configuration.getNumberOfNetworkInputs();

		if (numberOfInputs > 0) {
			CheckpointedInputGate inputGate = createCheckpointedInputGate();
			Counter numRecordsIn = setupNumRecordsInCounter(mainOperator);
			DataOutput<IN> output = createDataOutput(numRecordsIn);
			StreamTaskInput<IN> input = createTaskInput(inputGate);

			if (configuration.shouldSortInputs()) {
				checkState(!configuration.isCheckpointingEnabled(), "Checkpointing is not allowed with sorted inputs.");
				input = wrapWithSorted(input);
			}

			getEnvironment().getMetricGroup().getIOMetricGroup().reuseRecordsInputCounter(numRecordsIn);

			inputProcessor = new StreamOneInputProcessor<>(
				input,
				output,
				operatorChain);
		}
		mainOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.inputWatermarkGauge::getValue);
	}

	private StreamTaskInput<IN> wrapWithSorted(StreamTaskInput<IN> input) {
		ClassLoader userCodeClassLoader = getUserCodeClassLoader();
		return new SortingDataInput<>(
			input,
			configuration.getTypeSerializerIn(input.getInputIndex(), userCodeClassLoader),
			configuration.getStateKeySerializer(userCodeClassLoader),
			configuration.getStatePartitioner(input.getInputIndex(), userCodeClassLoader),
			getEnvironment().getMemoryManager(),
			getEnvironment().getIOManager(),
			getExecutionConfig().isObjectReuseEnabled(),
			configuration.getManagedMemoryFractionOperatorUseCaseOfSlot(
				ManagedMemoryUseCase.BATCH_OP,
				getTaskConfiguration(),
				userCodeClassLoader),
			getJobConfiguration(),
			this
		);
	}

	private CheckpointedInputGate createCheckpointedInputGate() {
		IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();

		return InputProcessorUtil.createCheckpointedInputGate(
			this,
			configuration,
			getCheckpointCoordinator(),
			inputGates,
			getEnvironment().getMetricGroup().getIOMetricGroup(),
			getTaskNameWithSubtaskAndId(),
			mainMailboxExecutor);
	}

	private DataOutput<IN> createDataOutput(Counter numRecordsIn) {
		return new StreamTaskNetworkOutput<>(
			mainOperator,
			getStreamStatusMaintainer(),
			inputWatermarkGauge,
			numRecordsIn);
	}

	private StreamTaskInput<IN> createTaskInput(CheckpointedInputGate inputGate) {
		int numberOfInputChannels = inputGate.getNumberOfInputChannels();
		StatusWatermarkValve statusWatermarkValve = new StatusWatermarkValve(numberOfInputChannels);

		TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());
		return new StreamTaskNetworkInput<>(
			inputGate,
			inSerializer,
			getEnvironment().getIOManager(),
			statusWatermarkValve,
			0);
	}

	/**
	 * The network data output implementation used for processing stream elements
	 * from {@link StreamTaskNetworkInput} in one input processor.
	 */
	private static class StreamTaskNetworkOutput<IN> extends AbstractDataOutput<IN> {

		private final OneInputStreamOperator<IN, ?> operator;

		private final WatermarkGauge watermarkGauge;
		private final Counter numRecordsIn;

		private StreamTaskNetworkOutput(
				OneInputStreamOperator<IN, ?> operator,
				StreamStatusMaintainer streamStatusMaintainer,
				WatermarkGauge watermarkGauge,
				Counter numRecordsIn) {
			super(streamStatusMaintainer);

			this.operator = checkNotNull(operator);
			this.watermarkGauge = checkNotNull(watermarkGauge);
			this.numRecordsIn = checkNotNull(numRecordsIn);
		}

		@Override
		public void emitRecord(StreamRecord<IN> record) throws Exception {
			numRecordsIn.inc();
			operator.setKeyContextElement1(record);
			operator.processElement(record);
		}

		@Override
		public void emitWatermark(Watermark watermark) throws Exception {
			watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
			operator.processWatermark(watermark);
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			operator.processLatencyMarker(latencyMarker);
		}
	}
}

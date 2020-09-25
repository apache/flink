/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.InputConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.runtime.io.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.InputProcessorUtil;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A {@link StreamTask} for executing a {@link MultipleInputStreamOperator} and supporting
 * the {@link MultipleInputStreamOperator} to select input for reading.
 */
@Internal
public class MultipleInputStreamTask<OUT> extends StreamTask<OUT, MultipleInputStreamOperator<OUT>> {
	private static final int MAX_TRACKED_CHECKPOINTS = 100_000;

	private final HashMap<Long, CompletableFuture<Boolean>> pendingCheckpointCompletedFutures = new HashMap<>();

	@Nullable
	private CheckpointBarrierHandler checkpointBarrierHandler;

	public MultipleInputStreamTask(Environment env) throws Exception {
		super(env);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();
		ClassLoader userClassLoader = getUserCodeClassLoader();

		InputConfig[] inputs = configuration.getInputs(userClassLoader);

		WatermarkGauge[] watermarkGauges = new WatermarkGauge[inputs.length];

		for (int i = 0; i < inputs.length; i++) {
			watermarkGauges[i] = new WatermarkGauge();
			mainOperator.getMetricGroup().gauge(MetricNames.currentInputWatermarkName(i + 1), watermarkGauges[i]);
		}

		MinWatermarkGauge minInputWatermarkGauge = new MinWatermarkGauge(watermarkGauges);
		mainOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);

		List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

		// Those two number may differ for example when one of the inputs is a union. In that case
		// the number of logical network inputs is smaller compared to the number of inputs (input gates)
		int numberOfNetworkInputs = configuration.getNumberOfNetworkInputs();
		int numberOfLogicalNetworkInputs = (int) Arrays.stream(inputs)
			.filter(input -> (input instanceof StreamConfig.NetworkInputConfig))
			.count();

		ArrayList[] inputLists = new ArrayList[numberOfLogicalNetworkInputs];
		for (int i = 0; i < inputLists.length; i++) {
			inputLists[i] = new ArrayList<>();
		}

		for (int i = 0; i < numberOfNetworkInputs; i++) {
			int inputType = inEdges.get(i).getTypeNumber();
			IndexedInputGate reader = getEnvironment().getInputGate(i);
			inputLists[inputType - 1].add(reader);
		}

		createInputProcessor(inputLists, inputs, watermarkGauges);

		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);
	}

	protected void createInputProcessor(
			List<IndexedInputGate>[] inputGates,
			InputConfig[] inputs,
			WatermarkGauge[] inputWatermarkGauges) {
		MultipleInputSelectionHandler selectionHandler = new MultipleInputSelectionHandler(
			mainOperator instanceof InputSelectable ? (InputSelectable) mainOperator : null,
			inputs.length);

		checkpointBarrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
			this,
			getConfiguration(),
			getCheckpointCoordinator(),
			getTaskNameWithSubtaskAndId(),
			inputGates,
			operatorChain.getSourceTaskInputs());

		CheckpointedInputGate[] checkpointedInputGates = InputProcessorUtil.createCheckpointedMultipleInputGate(
			mainMailboxExecutor,
			inputGates,
			getEnvironment().getMetricGroup().getIOMetricGroup(),
			checkpointBarrierHandler);

		inputProcessor = new StreamMultipleInputProcessor(
			checkpointedInputGates,
			inputs,
			getEnvironment().getIOManager(),
			getEnvironment().getMetricGroup().getIOMetricGroup(),
			setupNumRecordsInCounter(mainOperator),
			getStreamStatusMaintainer(),
			mainOperator,
			selectionHandler,
			inputWatermarkGauges,
			operatorChain);
	}

	@Override
	public Future<Boolean> triggerCheckpointAsync(
			CheckpointMetaData metadata,
			CheckpointOptions options,
			boolean advanceToEndOfEventTime) {

		CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();
		mainMailboxExecutor.execute(
			() -> {
				try {
					/**
					 * Contrary to {@link SourceStreamTask}, we are not using here
					 * {@link StreamTask#latestAsyncCheckpointStartDelayNanos} to measure the start delay
					 * metric, but we will be using {@link CheckpointBarrierHandler#getCheckpointStartDelayNanos()}
					 * instead.
					 */
					pendingCheckpointCompletedFutures.put(metadata.getCheckpointId(), resultFuture);
					checkPendingCheckpointCompletedFuturesSize();
					triggerSourcesCheckpoint(new CheckpointBarrier(metadata.getCheckpointId(), metadata.getTimestamp(), options));
				}
				catch (Exception ex) {
					// Report the failure both via the Future result but also to the mailbox
					pendingCheckpointCompletedFutures.remove(metadata.getCheckpointId());
					resultFuture.completeExceptionally(ex);
					throw ex;
				}
			},
			"checkpoint %s with %s",
			metadata,
			options);
		return resultFuture;
	}

	private void checkPendingCheckpointCompletedFuturesSize() {
		if (pendingCheckpointCompletedFutures.size() > MAX_TRACKED_CHECKPOINTS) {
			ArrayList<Long> pendingCheckpointIds = new ArrayList<>(pendingCheckpointCompletedFutures.keySet());
			pendingCheckpointIds.sort(Long::compareTo);
			for (Long checkpointId : pendingCheckpointIds.subList(0, pendingCheckpointIds.size() - MAX_TRACKED_CHECKPOINTS)) {
				pendingCheckpointCompletedFutures.remove(checkpointId).completeExceptionally(new IllegalStateException("Too many pending checkpoints"));
			}
		}
	}

	private void triggerSourcesCheckpoint(CheckpointBarrier checkpointBarrier) throws IOException {
		for (StreamTaskSourceInput<?> sourceInput : operatorChain.getSourceTaskInputs()) {
			for (InputChannelInfo channelInfo : sourceInput.getChannelInfos()) {
				checkpointBarrierHandler.processBarrier(checkpointBarrier, channelInfo);
			}
		}
	}

	@Override
	public void triggerCheckpointOnBarrier(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws IOException {
		CompletableFuture<Boolean> resultFuture = pendingCheckpointCompletedFutures.remove(checkpointMetaData.getCheckpointId());
		try {
			super.triggerCheckpointOnBarrier(checkpointMetaData, checkpointOptions, checkpointMetrics);
			if (resultFuture != null) {
				resultFuture.complete(true);
			}
		}
		catch (IOException ex) {
			if (resultFuture != null) {
				resultFuture.completeExceptionally(ex);
			}
			throw ex;
		}
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws IOException {
		CompletableFuture<Boolean> resultFuture = pendingCheckpointCompletedFutures.remove(checkpointId);
		if (resultFuture != null) {
			resultFuture.completeExceptionally(cause);
		}
		super.abortCheckpointOnBarrier(checkpointId, cause);
	}
}

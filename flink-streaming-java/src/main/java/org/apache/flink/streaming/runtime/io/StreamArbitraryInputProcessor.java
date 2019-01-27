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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSourceV2;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.InputFetcher.InputFetcherAvailableListener;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;
import org.apache.flink.streaming.runtime.tasks.InputSelector;
import org.apache.flink.streaming.runtime.tasks.InputSelector.EdgeInputSelection;
import org.apache.flink.streaming.runtime.tasks.InputSelector.InputSelection;
import org.apache.flink.streaming.runtime.tasks.InputSelector.SelectionChangedListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.streaming.runtime.tasks.InputSelector.SourceInputSelection;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.ArbitraryInputStreamTask}.
 */
@Internal
public class StreamArbitraryInputProcessor implements SelectionChangedListener, InputFetcherAvailableListener {

	private static final Logger LOG = LoggerFactory.getLogger(StreamArbitraryInputProcessor.class);

	private volatile boolean isStopped = false;

	private IOManager ioManager;

	private final Object checkpointLock;

	private final InputSelector inputSelector;

	private final TaskMetricGroup taskMetricGroup;

	private final MinWatermarkGauge minAllInputWatermarkGauge;

	private final SelectedReadingBarrierHandler barrierHandler;

	private final ArrayDeque<InputFetcher> inputFetcherReadingQueue = new ArrayDeque<>();

	private final Set<InputFetcher> enqueuedInputFetchers = new HashSet<>();

	private final List<InputFetcher> inputFetchers = new ArrayList<>();

	private final Set<InputFetcher> selectedInputFetchers = new HashSet<>();

	private volatile boolean inputSelectionChanged = false;

	public StreamArbitraryInputProcessor(
		IOManager ioManager,
		Object checkpointLock,
		InputSelector inputSelector,
		TaskMetricGroup taskMetricGroup,
		@Nullable SelectedReadingBarrierHandler barrierHandler) {

		this.ioManager = checkNotNull(ioManager);
		this.checkpointLock = checkNotNull(checkpointLock);
		this.inputSelector = checkNotNull(inputSelector);
		this.taskMetricGroup = checkNotNull(taskMetricGroup);
		this.barrierHandler = barrierHandler;

		this.inputSelector.registerSelectionChangedListener(this);

		minAllInputWatermarkGauge = new MinWatermarkGauge();
		this.taskMetricGroup.gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minAllInputWatermarkGauge::getValue);
	}

	public void process() throws Exception {

		LOG.info("Start processing with {} source(s)/input gate(s)", inputFetchers.size());

		for (InputFetcher inputFetcher : inputFetchers) {
			inputFetcher.setup();
		}

		while (!isStopped && !inputFetchers.isEmpty()) {
			// Put the input fetchers that would be read into a queue
			constructInputFetcherReadingQueue();

			while (!inputSelectionChanged && !inputFetchers.isEmpty()) {
				checkState(!selectedInputFetchers.isEmpty());
				final InputFetcher inputFetcher;

				synchronized (inputFetcherReadingQueue) {
					while (inputFetcherReadingQueue.isEmpty()) {
						inputFetcherReadingQueue.wait();
					}
					inputFetcher = dequeueInputFetcher();
				}

				// The input fetcher would return false after processing some records for sharing cpu with other fetchers
				while (!inputSelectionChanged && inputFetcher.fetchAndProcess()) {}

				if (inputFetcher.isFinished()) {
					LOG.info("Input fetcher {} {} is finished", inputFetcher, inputFetcher.getInputSelection());
					inputFetcher.cleanup();
					inputFetchers.remove(inputFetcher);
					selectedInputFetchers.remove(inputFetcher);
				} else if (!inputSelectionChanged && inputFetcher.moreAvailable()) {
					// This input fetcher has more available datum, we need enqueue it
					// Otherwise, we need to wait the available listener
					enqueueInputFetcher(inputFetcher);
				}
			}
			if (inputSelectionChanged) {
				LOG.info("Input selection is changed, need to reconstruct reading queue");
			}
		}
		if (isStopped) {
			for (InputFetcher inputFetcher : inputFetchers) {
				inputFetcher.cancel();
			}
		}

		LOG.info("Finish processing all input fetchers");
	}

	public void stop() {
		this.isStopped = true;
	}

	public void cleanup() throws Exception {
		for (InputFetcher inputFetcher : inputFetchers) {
			inputFetcher.cleanup();
		}

		// cleanup the barrier handler resources
		if (barrierHandler != null) {
			barrierHandler.cleanup();
		}
	}

	public <IN> void bindOneInputOperator(
		StreamEdge streamEdge,
		InputGate inputGate,
		int maxChannelIndex,
		OneInputStreamOperator<IN, ?> operator,
		TypeSerializer<IN> typeSerializer,
		StreamStatusSubMaintainer streamStatusSubMaintainer,
		boolean isObjectReuse,
		Configuration taskManagerConfig) {

		checkNotNull(barrierHandler);

		final InputGateFetcher<IN> inputGateFetcher = new InputGateFetcher<>(
			EdgeInputSelection.create(streamEdge),
			inputGate,
			typeSerializer,
			barrierHandler,
			ioManager,
			new OneInputProcessor(
				streamStatusSubMaintainer,
				operator,
				checkpointLock,
				taskMetricGroup,
				minAllInputWatermarkGauge,
				inputGate.getNumberOfInputChannels()),
			checkpointLock,
			maxChannelIndex,
			isObjectReuse,
			taskManagerConfig);
		inputGateFetcher.registerAvailableListener(this);

		inputFetchers.add(inputGateFetcher);

		LOG.info("Bind an one input operator {} to {}", operator, inputGate);
	}

	public <IN> void bindFirstOfTwoInputOperator(
		StreamEdge streamEdge,
		InputGate inputGate,
		int maxChannelIndex,
		TwoInputStreamOperator<IN, ?, ?> operator,
		TypeSerializer<IN> typeSerializer,
		StreamStatusSubMaintainer streamStatusSubMaintainer,
		boolean isObjectReuse,
		Configuration taskManagerConfig) {

		checkNotNull(barrierHandler);

		final InputGateFetcher<IN> inputGateFetcher = new InputGateFetcher<>(
			EdgeInputSelection.create(streamEdge),
			inputGate,
			typeSerializer,
			barrierHandler,
			ioManager,
			new FirstOfTwoInputProcessor(
				streamStatusSubMaintainer,
				operator,
				checkpointLock,
				taskMetricGroup,
				minAllInputWatermarkGauge,
				inputGate.getNumberOfInputChannels()),
			checkpointLock,
			maxChannelIndex,
			isObjectReuse,
			taskManagerConfig);
		inputGateFetcher.registerAvailableListener(this);

		inputFetchers.add(inputGateFetcher);

		LOG.info("Bind the edge {} of a first of two input operator {} to {}", streamEdge, operator, inputGate);
	}

	public <IN> void bindSecondOfTwoInputOperator(
		StreamEdge streamEdge,
		InputGate inputGate,
		int maxChannelIndex,
		TwoInputStreamOperator<IN, ?, ?> operator,
		TypeSerializer<IN> typeSerializer,
		StreamStatusSubMaintainer streamStatusSubMaintainer,
		boolean isObjectReuse,
		Configuration taskManagerConfig) {

		checkNotNull(barrierHandler);

		final InputGateFetcher<IN> inputGateFetcher = new InputGateFetcher<>(
			EdgeInputSelection.create(streamEdge),
			inputGate,
			typeSerializer,
			barrierHandler,
			ioManager,
			new SecondOfTwoInputProcessor(
				streamStatusSubMaintainer,
				operator,
				checkpointLock,
				taskMetricGroup,
				minAllInputWatermarkGauge,
				inputGate.getNumberOfInputChannels()),
			checkpointLock,
			maxChannelIndex,
			isObjectReuse,
			taskManagerConfig);
		inputGateFetcher.registerAvailableListener(this);

		inputFetchers.add(inputGateFetcher);

		LOG.info("Bind the edge {} of a second of two input operator {} to {}", streamEdge, operator, inputGate);
	}

	public <IN> void bindSourceOperator(
		int operatorId,
		StreamSourceV2 operator,
		OneInputStreamOperator<IN, ?> operatorProxy,
		SourceContext context,
		StreamStatusSubMaintainer streamStatusSubMaintainer) {

		final SourceFetcher sourceFetcher = new SourceFetcher(
			SourceInputSelection.create(operatorId),
			operator,
			context,
			new SourceInputProcessor(
				streamStatusSubMaintainer,
				operatorProxy,
				checkpointLock,
				taskMetricGroup,
				1));
		sourceFetcher.registerAvailableListener(this);

		inputFetchers.add(sourceFetcher);

		LOG.info("Bind a source operator {}", operatorId);
	}

	@VisibleForTesting
	void constructInputFetcherReadingQueue() {
		synchronized (inputFetcherReadingQueue) {
			// Reset input selection
			inputSelectionChanged = false;
			selectedInputFetchers.clear();
			// Reset reading queue
			inputFetcherReadingQueue.clear();
			enqueuedInputFetchers.clear();

			final List<InputSelection> inputSelections = inputSelector.getNextSelectedInputs();
			checkNotNull(inputSelections);
			checkState(!inputSelections.isEmpty());

			final Map<InputSelection, InputFetcher> inputFetcherMap = new HashMap<>();
			inputFetchers.forEach(inputFetcher -> inputFetcherMap.put(inputFetcher.getInputSelection(), inputFetcher));

			for (InputSelection inputSelection : inputSelections) {
				final InputFetcher inputFetcher = inputFetcherMap.get(inputSelection);
				if (inputFetcher != null) {
					selectedInputFetchers.add(inputFetcher);
					// SourceFetcher may have no data in the beginning, we have to enqueue it at first time.
					if (inputFetcher.moreAvailable() || inputFetcher.isFinished()) {
						enqueueInputFetcher(inputFetcher);
					}
				} else {
					LOG.info("The selected edge {} is finished already", inputSelection);
				}
			}
			LOG.info("Select inputs {}", inputSelections);
		}
	}

	@VisibleForTesting
	void enqueueInputFetcher(InputFetcher inputFetcher) {
		synchronized (inputFetcherReadingQueue) {
			if (!enqueuedInputFetchers.contains(inputFetcher)) {
				inputFetcherReadingQueue.add(inputFetcher);
				enqueuedInputFetchers.add(inputFetcher);
			}
		}
	}

	@VisibleForTesting
	InputFetcher dequeueInputFetcher() {
		synchronized (inputFetcherReadingQueue) {
			final InputFetcher inputFetcher = inputFetcherReadingQueue.remove();
			enqueuedInputFetchers.remove(inputFetcher);
			return inputFetcher;
		}
	}

	@VisibleForTesting
	List<InputFetcher> getInputFetchers() {
		return inputFetchers;
	}

	@Override
	public void notifySelectionChanged() {
		inputSelectionChanged = true;
	}

	@VisibleForTesting
	ArrayDeque<InputFetcher> getInputFetcherReadingQueue() {
		return inputFetcherReadingQueue;
	}

	@VisibleForTesting
	Set<InputFetcher> getEnqueuedInputFetchers() {
		return enqueuedInputFetchers;
	}

	@Override
	public void notifyInputFetcherAvailable(InputFetcher inputFetcher) {
		synchronized (inputFetcherReadingQueue) {
			if (selectedInputFetchers.contains(inputFetcher) &&
				!enqueuedInputFetchers.contains(inputFetcher)) {

				boolean noDataAvailable = inputFetcherReadingQueue.isEmpty();

				inputFetcherReadingQueue.add(inputFetcher);
				enqueuedInputFetchers.add(inputFetcher);

				if (noDataAvailable) {
					inputFetcherReadingQueue.notifyAll();
				}
			}
		}
	}
}

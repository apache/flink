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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.operators.python.aggregate.arrow.AbstractArrowPythonAggregateFunctionOperator;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.internal.InternalWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * The Stream Arrow Python {@link AggregateFunction} Operator for Group Window Aggregation.
 */
@Internal
public class StreamArrowPythonGroupWindowAggregateFunctionOperator<K, W extends Window>
	extends AbstractArrowPythonAggregateFunctionOperator implements Triggerable<K, W> {

	private static final long serialVersionUID = 1L;

	/**
	 * The Infos of the Window.
	 * 0 -> start of the Window.
	 * 1 -> end of the Window.
	 * 2 -> row time of the Window.
	 */
	private final int[] namedProperties;

	/**
	 * The row time index of the input data.
	 */
	private final int inputTimeFieldIndex;

	/**
	 * A {@link WindowAssigner} assigns zero or more {@link Window Windows} to an element.
	 */
	private final WindowAssigner<W> windowAssigner;

	/**
	 * A {@link Trigger} determines when a pane of a window should be evaluated to emit the
	 * results for that part of the window.
	 */
	private final Trigger<W> trigger;

	/**
	 * The allowed lateness for elements. This is used for:
	 * <ul>
	 * 		<li>Deciding if an element should be dropped from a window due to lateness.
	 * 		<li>Clearing the state of a window if the system time passes the
	 * 		{@code window.maxTimestamp + allowedLateness} landmark.
	 * </ul>
	 */
	private final long allowedLateness;

	/**
	 * Interface for working with time and timers.
	 */
	private transient InternalTimerService<W> internalTimerService;

	/**
	 * Stores accumulate message data(INSERT/UPDATE_AFTER) in window.
	 */
	private transient InternalListState<K, W, RowData> windowAccumulateData;

	/**
	 * Stores retract message data(DELETE/UPDATE_BEFORE) in window.
	 */
	private transient InternalListState<K, W, RowData> windowRetractData;

	private transient TriggerContext triggerContext;

	/**
	 * For serializing the window in checkpoints.
	 */
	private transient TypeSerializer<W> windowSerializer;

	/**
	 * The queue holding the input groupSet with the Window for which the execution results
	 * have not been received.
	 */
	private transient LinkedList<Tuple2<RowData, W>> inputKeyAndWindow;

	/**
	 * The GenericRowData reused holding the property of the window, such as window start, window
	 * end and window time.
	 */
	private transient GenericRowData windowProperty;

	/**
	 * The JoinedRowData reused holding the window agg execution result.
	 */
	private transient JoinedRowData windowAggResult;

	private transient long timestamp;

	private transient Collection<W> elementWindows;

	public StreamArrowPythonGroupWindowAggregateFunctionOperator(
		Configuration config,
		PythonFunctionInfo[] pandasAggFunctions,
		RowType inputType,
		RowType outputType,
		int inputTimeFieldIndex,
		WindowAssigner<W> windowAssigner,
		Trigger<W> trigger,
		long allowedLateness,
		int[] namedProperties,
		int[] groupingSet,
		int[] udafInputOffsets) {
		super(config, pandasAggFunctions, inputType, outputType, groupingSet, udafInputOffsets);
		this.namedProperties = namedProperties;
		this.inputTimeFieldIndex = inputTimeFieldIndex;
		this.windowAssigner = windowAssigner;
		this.trigger = trigger;
		this.allowedLateness = allowedLateness;
	}

	@Override
	public void open() throws Exception {
		userDefinedFunctionOutputType = new RowType(
			outputType.getFields().subList(groupingSet.length, outputType.getFieldCount() - namedProperties.length));
		windowSerializer = windowAssigner.getWindowSerializer(new ExecutionConfig());

		internalTimerService = getInternalTimerService("window-timers", windowSerializer, this);

		triggerContext = new TriggerContext();
		triggerContext.open();

		StateDescriptor<ListState<RowData>, List<RowData>> windowStateDescriptor = new ListStateDescriptor<>(
			"window-input",
			new RowDataSerializer(inputType));
		StateDescriptor<ListState<RowData>, List<RowData>> dataRetractStateDescriptor = new ListStateDescriptor<>(
			"data-retract",
			new RowDataSerializer(inputType));
		this.windowAccumulateData = (InternalListState<K, W, RowData>) getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
		this.windowRetractData = (InternalListState<K, W, RowData>) getOrCreateKeyedState(windowSerializer, dataRetractStateDescriptor);
		inputKeyAndWindow = new LinkedList<>();
		windowProperty = new GenericRowData(namedProperties.length);
		windowAggResult = new JoinedRowData();

		WindowContext windowContext = new WindowContext();
		windowAssigner.open(windowContext);
		super.open();
	}

	@Override
	public void bufferInput(RowData input) throws Exception {
		if (windowAssigner.isEventTime()) {
			timestamp = input.getLong(inputTimeFieldIndex);
		} else {
			timestamp = internalTimerService.currentProcessingTime();
		}
		// Given the timestamp and element, returns the set of windows into which it
		// should be placed.
		elementWindows = windowAssigner.assignWindows(input, timestamp);
		for (W window : elementWindows) {
			if (RowDataUtil.isAccumulateMsg(input)) {
				windowAccumulateData.setCurrentNamespace(window);
				windowAccumulateData.add(input);
			} else {
				windowRetractData.setCurrentNamespace(window);
				windowRetractData.add(input);
			}
		}
	}

	@Override
	public void processElementInternal(RowData value) throws Exception {
		List<W> actualWindows = new ArrayList<>(elementWindows.size());
		for (W window : elementWindows) {
			if (!isWindowLate(window)) {
				actualWindows.add(window);
			}
		}
		for (W window : actualWindows) {
			triggerContext.window = window;
			boolean triggerResult = triggerContext.onElement(value, timestamp);
			if (triggerResult) {
				triggerWindowProcess(window);
			}
			// register a clean up timer for the window
			registerCleanupTimer(window);
		}
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] udafResult = resultTuple.f0;
		int length = resultTuple.f1;
		bais.setBuffer(udafResult, 0, length);
		int rowCount = arrowSerializer.load();
		for (int i = 0; i < rowCount; i++) {
			Tuple2<RowData, W> input = inputKeyAndWindow.poll();
			RowData key = input.f0;
			W window = input.f1;
			setWindowProperty(window);
			windowAggResult.replace(key, arrowSerializer.read(i));
			rowDataWrapper.collect(reuseJoinedRow.replace(windowAggResult, windowProperty));
		}
	}

	@Override
	public void onEventTime(InternalTimer<K, W> timer) throws Exception {
		setCurrentKey(timer.getKey());

		triggerContext.window = timer.getNamespace();
		if (triggerContext.onEventTime(timer.getTimestamp())) {
			// fire
			triggerWindowProcess(triggerContext.window);
		}

		if (windowAssigner.isEventTime()) {
			cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		setCurrentKey(timer.getKey());

		triggerContext.window = timer.getNamespace();
		if (triggerContext.onProcessingTime(timer.getTimestamp())) {
			// fire
			triggerWindowProcess(triggerContext.window);
		}

		if (!windowAssigner.isEventTime()) {
			cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
		}
	}

	/**
	 * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness
	 * of the given window.
	 */
	private boolean isWindowLate(W window) {
		return windowAssigner.isEventTime() &&
			(cleanupTime(window) <= internalTimerService.currentWatermark());
	}

	/**
	 * Returns the cleanup time for a window, which is
	 * {@code window.maxTimestamp + allowedLateness}. In
	 * case this leads to a value greated than {@link Long#MAX_VALUE}
	 * then a cleanup time of {@link Long#MAX_VALUE} is
	 * returned.
	 *
	 * @param window the window whose cleanup time we are computing.
	 */
	private long cleanupTime(W window) {
		if (windowAssigner.isEventTime()) {
			long cleanupTime = window.maxTimestamp() + allowedLateness;
			return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
		} else {
			return window.maxTimestamp();
		}
	}

	private void triggerWindowProcess(W window) throws Exception {
		windowAccumulateData.setCurrentNamespace(window);
		windowRetractData.setCurrentNamespace(window);
		Iterable<RowData> currentWindowAccumulateData = windowAccumulateData.get();
		Iterable<RowData> currentWindowRetractData = windowRetractData.get();
		if (currentWindowAccumulateData != null) {
			currentBatchCount = 0;
			for (RowData accumulateData : currentWindowAccumulateData) {
				if (!hasRetractData(accumulateData, currentWindowRetractData)) {
					arrowSerializer.write(getFunctionInput(accumulateData));
					currentBatchCount++;
				}
			}
			if (currentBatchCount > 0) {
				inputKeyAndWindow.add(Tuple2.of((RowData) getCurrentKey(), window));
				arrowSerializer.finishCurrentBatch();
				pythonFunctionRunner.process(baos.toByteArray());
				elementCount += currentBatchCount;
				checkInvokeFinishBundleByCount();
				currentBatchCount = 0;
				baos.reset();
			}
		}
	}

	private boolean hasRetractData(RowData accumulateData, Iterable<RowData> currentWindowRetractData) {
		BinaryRowData binaryAccumulateRowData = (BinaryRowData) accumulateData;
		if (currentWindowRetractData != null) {
			for (RowData retractData : currentWindowRetractData) {
				if (retractData.getRowKind() == RowKind.UPDATE_BEFORE) {
					retractData.setRowKind(RowKind.UPDATE_AFTER);
				} else {
					retractData.setRowKind(RowKind.INSERT);
				}
				BinaryRowData binaryRetractData = (BinaryRowData) retractData;
				if (binaryAccumulateRowData.getSizeInBytes() == binaryRetractData.getSizeInBytes() &&
					BinaryRowDataUtil.byteArrayEquals(
						binaryAccumulateRowData.getSegments()[0].getHeapMemory(),
						binaryRetractData.getSegments()[0].getHeapMemory(),
						binaryAccumulateRowData.getSizeInBytes())) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Registers a timer to cleanup the content of the window.
	 *
	 * @param window the window whose state to discard
	 */
	private void registerCleanupTimer(W window) {
		long cleanupTime = cleanupTime(window);
		if (cleanupTime == Long.MAX_VALUE) {
			// don't set a GC timer for "end of time"
			return;
		}

		if (windowAssigner.isEventTime()) {
			triggerContext.registerEventTimeTimer(cleanupTime);
		} else {
			triggerContext.registerProcessingTimeTimer(cleanupTime);
		}
	}

	private void setWindowProperty(W currentWindow) {
		for (int i = 0; i < namedProperties.length; i++) {
			switch (namedProperties[i]) {
				case 0:
					windowProperty.setField(i, TimestampData.fromEpochMillis(((TimeWindow) currentWindow).getStart()));
					break;
				case 1:
					windowProperty.setField(i, TimestampData.fromEpochMillis(((TimeWindow) currentWindow).getEnd()));
					break;
				case 2:
					windowProperty.setField(i, TimestampData.fromEpochMillis(currentWindow.maxTimestamp()));
					break;
			}
		}
	}

	private void cleanWindowIfNeeded(W window, long currentTime) throws Exception {
		if (currentTime == cleanupTime(window)) {
			windowAccumulateData.setCurrentNamespace(window);
			windowAccumulateData.clear();
			windowRetractData.setCurrentNamespace(window);
			windowRetractData.clear();
			triggerContext.window = window;
			triggerContext.clear();
		}
	}

	/**
	 * {@code TriggerContext} is a utility for handling {@code Trigger} invocations. It can be
	 * reused by setting the {@code key} and {@code window} fields. No internal state must be kept
	 * in the {@code TriggerContext}.
	 */
	private class TriggerContext implements Trigger.TriggerContext {

		private W window;

		public void open() throws Exception {
			trigger.open(this);
		}

		boolean onElement(RowData row, long timestamp) throws Exception {
			return trigger.onElement(row, timestamp, window);
		}

		boolean onProcessingTime(long time) throws Exception {
			return trigger.onProcessingTime(time, window);
		}

		boolean onEventTime(long time) throws Exception {
			return trigger.onEventTime(time, window);
		}

		void clear() throws Exception {
			trigger.clear(window);
		}

		@Override
		public long getCurrentProcessingTime() {
			return internalTimerService.currentProcessingTime();
		}

		@Override
		public long getCurrentWatermark() {
			return internalTimerService.currentWatermark();
		}

		@Override
		public MetricGroup getMetricGroup() {
			return StreamArrowPythonGroupWindowAggregateFunctionOperator.this.getMetricGroup();
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			internalTimerService.registerProcessingTimeTimer(window, time);
		}

		@Override
		public void registerEventTimeTimer(long time) {
			internalTimerService.registerEventTimeTimer(window, time);
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			internalTimerService.deleteProcessingTimeTimer(window, time);
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			internalTimerService.deleteEventTimeTimer(window, time);
		}

		@Override
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return StreamArrowPythonGroupWindowAggregateFunctionOperator.this.
					getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}
	}

	private class WindowContext implements InternalWindowProcessFunction.Context<K, W> {
		@Override
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
			return StreamArrowPythonGroupWindowAggregateFunctionOperator.this.
				getPartitionedState(stateDescriptor);
		}

		@Override
		public K currentKey() {
			throw new RuntimeException("The method currentKey should not be called.");
		}

		@Override
		public long currentProcessingTime() {
			throw new RuntimeException("The method currentProcessingTime should not be called.");
		}

		@Override
		public long currentWatermark() {
			throw new RuntimeException("The method currentWatermark should not be called.");
		}

		@Override
		public RowData getWindowAccumulators(W window) {
			throw new RuntimeException("The method getWindowAccumulators should not be called.");
		}

		@Override
		public void setWindowAccumulators(W window, RowData acc) {
			throw new RuntimeException("The method setWindowAccumulators should not be called.");
		}

		@Override
		public void clearWindowState(W window) {
			throw new RuntimeException("The method clearWindowState should not be called.");
		}

		@Override
		public void clearPreviousState(W window) {
			throw new RuntimeException("The method clearPreviousState should not be called.");
		}

		@Override
		public void clearTrigger(W window) {
			throw new RuntimeException("The method clearTrigger should not be called.");
		}

		@Override
		public void onMerge(W newWindow, Collection<W> mergedWindows) {
			throw new RuntimeException("The method onMerge should not be called.");
		}

		@Override
		public void deleteCleanupTimer(W window) {
			throw new RuntimeException("The method deleteCleanupTimer should not be called.");
		}
	}
}

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

package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.dataview.PerWindowStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.window.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.PanedWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.internal.GeneralWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.InternalWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.MergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.PanedWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Collection;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that implements the logic for windowing based on a {@link WindowAssigner} and
 * {@link Trigger}.
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes if it was assigned to multiple windows by
 * the
 * {@code WindowAssigner}.
 *
 * <p>Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires,
 * the given {@link org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction}
 * is invoked to produce the results that are emitted for the pane to which the {@code Trigger}
 * belongs.
 *
 * <p>The parameter types:
 * {@code <IN>}: BaseRow
 * {@code <OUT>}: JoinedRow(KEY, AGG_RESULT)
 * {@code <KEY>}: GenericRow
 * {@code <AGG_RESULT>}: GenericRow
 * {@code <ACC>}: GenericRow
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class WindowOperator<K, W extends Window>
		extends AbstractStreamOperator<BaseRow>
		implements OneInputStreamOperator<BaseRow, BaseRow>, Triggerable<K, W> {

	private static final long serialVersionUID = 1L;

	private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
	private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
	private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	private final WindowAssigner<W> windowAssigner;

	private final Trigger<W> trigger;

	/** For serializing the window in checkpoints. */
	private final TypeSerializer<W> windowSerializer;

	private final LogicalType[] inputFieldTypes;

	private final LogicalType[] accumulatorTypes;

	private final LogicalType[] aggResultTypes;

	private final LogicalType[] windowPropertyTypes;

	private final boolean sendRetraction;

	private final int rowtimeIndex;

	/**
	 * The allowed lateness for elements. This is used for:
	 * <ul>
	 * <li>Deciding if an element should be dropped from a window due to lateness.
	 * <li>Clearing the state of a window if the system time passes the
	 * {@code window.maxTimestamp + allowedLateness} landmark.
	 * </ul>
	 */
	private final long allowedLateness;

	// --------------------------------------------------------------------------------

	private NamespaceAggsHandleFunction<W> windowAggregator;
	private GeneratedNamespaceAggsHandleFunction<W> generatedWindowAggregator;

	/**
	 * The util to compare two BaseRow equals to each other.
	 * As different BaseRow can't be equals directly, we use a code generated util to handle this.
	 */
	private RecordEqualiser equaliser;
	private GeneratedRecordEqualiser generatedEqualiser;

	// --------------------------------------------------------------------------------

	private transient InternalWindowProcessFunction<K, W> windowFunction;

	/** This is used for emitting elements with a given timestamp. */
	private transient TimestampedCollector<BaseRow> collector;

	/** Flag to prevent duplicate function.close() calls in close() and dispose(). */
	private transient boolean functionsClosed = false;

	private transient InternalTimerService<W> internalTimerService;

	private transient InternalValueState<K, W, BaseRow> windowState;

	private transient InternalValueState<K, W, BaseRow> previousState;

	private transient TriggerContext triggerContext;

	private transient JoinedRow reuseOutput;

	// ------------------------------------------------------------------------
	// Metrics
	// ------------------------------------------------------------------------

	private transient Counter numLateRecordsDropped;
	private transient Meter lateRecordsDroppedRate;
	private transient Gauge<Long> watermarkLatency;

	WindowOperator(
			NamespaceAggsHandleFunction<W> windowAggregator,
			RecordEqualiser equaliser,
			WindowAssigner<W> windowAssigner,
			Trigger<W> trigger,
			TypeSerializer<W> windowSerializer,
			LogicalType[] inputFieldTypes,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes,
			int rowtimeIndex,
			boolean sendRetraction,
			long allowedLateness) {
		checkArgument(allowedLateness >= 0);
		this.windowAggregator = checkNotNull(windowAggregator);
		this.equaliser = checkNotNull(equaliser);
		this.windowAssigner = checkNotNull(windowAssigner);
		this.trigger = checkNotNull(trigger);
		this.windowSerializer = checkNotNull(windowSerializer);
		this.inputFieldTypes = checkNotNull(inputFieldTypes);
		this.accumulatorTypes = checkNotNull(accumulatorTypes);
		this.aggResultTypes = checkNotNull(aggResultTypes);
		this.windowPropertyTypes = checkNotNull(windowPropertyTypes);
		this.allowedLateness = allowedLateness;
		this.sendRetraction = sendRetraction;

		// rowtime index should >= 0 when in event time mode
		checkArgument(!windowAssigner.isEventTime() || rowtimeIndex >= 0);
		this.rowtimeIndex = rowtimeIndex;

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	WindowOperator(
			GeneratedNamespaceAggsHandleFunction<W> generatedWindowAggregator,
			GeneratedRecordEqualiser generatedEqualiser,
			WindowAssigner<W> windowAssigner,
			Trigger<W> trigger,
			TypeSerializer<W> windowSerializer,
			LogicalType[] inputFieldTypes,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes,
			int rowtimeIndex,
			boolean sendRetraction,
			long allowedLateness) {
		checkArgument(allowedLateness >= 0);
		this.generatedWindowAggregator = checkNotNull(generatedWindowAggregator);
		this.generatedEqualiser = checkNotNull(generatedEqualiser);
		this.windowAssigner = checkNotNull(windowAssigner);
		this.trigger = checkNotNull(trigger);
		this.windowSerializer = checkNotNull(windowSerializer);
		this.inputFieldTypes = checkNotNull(inputFieldTypes);
		this.accumulatorTypes = checkNotNull(accumulatorTypes);
		this.aggResultTypes = checkNotNull(aggResultTypes);
		this.windowPropertyTypes = checkNotNull(windowPropertyTypes);
		this.allowedLateness = allowedLateness;
		this.sendRetraction = sendRetraction;

		// rowtime index should >= 0 when in event time mode
		checkArgument(!windowAssigner.isEventTime() || rowtimeIndex >= 0);
		this.rowtimeIndex = rowtimeIndex;

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void open() throws Exception {
		super.open();

		collector = new TimestampedCollector<>(output);
		collector.eraseTimestamp();

		internalTimerService = getInternalTimerService("window-timers", windowSerializer, this);

		triggerContext = new TriggerContext();
		triggerContext.open();

		StateDescriptor<ValueState<BaseRow>, BaseRow> windowStateDescriptor = new ValueStateDescriptor<>(
				"window-aggs",
				new BaseRowSerializer(getExecutionConfig(), accumulatorTypes));
		this.windowState = (InternalValueState<K, W, BaseRow>) getOrCreateKeyedState(windowSerializer, windowStateDescriptor);

		if (sendRetraction) {
			LogicalType[] valueTypes = ArrayUtils.addAll(aggResultTypes, windowPropertyTypes);
			StateDescriptor<ValueState<BaseRow>, BaseRow> previousStateDescriptor = new ValueStateDescriptor<>(
					"previous-aggs",
					new BaseRowSerializer(getExecutionConfig(), valueTypes));
			this.previousState = (InternalValueState<K, W, BaseRow>) getOrCreateKeyedState(windowSerializer, previousStateDescriptor);
		}

		// compile aggregator
		if (generatedWindowAggregator != null) {
			this.windowAggregator = generatedWindowAggregator.newInstance(getRuntimeContext().getUserCodeClassLoader());

		}
		// compile equaliser
		if (generatedEqualiser != null) {
			this.equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
		}

		WindowContext windowContext = new WindowContext();
		windowAggregator.open(new PerWindowStateDataViewStore(
			getKeyedStateBackend(),
			windowSerializer,
			getRuntimeContext()));

		if (windowAssigner instanceof MergingWindowAssigner) {
			this.windowFunction = new MergingWindowProcessFunction<>(
					(MergingWindowAssigner<W>) windowAssigner,
					windowAggregator,
					windowSerializer,
					allowedLateness);
		} else if (windowAssigner instanceof PanedWindowAssigner) {
			this.windowFunction = new PanedWindowProcessFunction<>(
					(PanedWindowAssigner<W>) windowAssigner,
					windowAggregator,
					allowedLateness);
		} else {
			this.windowFunction = new GeneralWindowProcessFunction<>(
					windowAssigner,
					windowAggregator,
					allowedLateness);
		}
		windowFunction.open(windowContext);

		reuseOutput = new JoinedRow();

		// metrics
		this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
		this.lateRecordsDroppedRate = metrics.meter(
				LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
				new MeterView(numLateRecordsDropped, 60));
		this.watermarkLatency = metrics.gauge(WATERMARK_LATENCY_METRIC_NAME, () -> {
			long watermark = internalTimerService.currentWatermark();
			if (watermark < 0) {
				return 0L;
			} else {
				return internalTimerService.currentProcessingTime() - watermark;
			}
		});
	}

	@Override
	public void close() throws Exception {
		super.close();
		collector = null;
		triggerContext = null;
		functionsClosed = true;
		windowAggregator.close();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		collector = null;
		triggerContext = null;
		if (!functionsClosed) {
			functionsClosed = true;
			windowAggregator.close();
		}
	}

	@Override
	public void processElement(StreamRecord<BaseRow> record) throws Exception {
		BaseRow inputRow = record.getValue();
		long timestamp;
		if (windowAssigner.isEventTime()) {
			timestamp = inputRow.getLong(rowtimeIndex);
		} else {
			timestamp = internalTimerService.currentProcessingTime();
		}

		// the windows which the input row should be placed into
		Collection<W> affectedWindows = windowFunction.assignStateNamespace(inputRow, timestamp);
		boolean isElementDropped = true;
		for (W window : affectedWindows) {
			isElementDropped = false;

			windowState.setCurrentNamespace(window);
			BaseRow acc = windowState.value();
			if (acc == null) {
				acc = windowAggregator.createAccumulators();
			}
			windowAggregator.setAccumulators(window, acc);

			if (BaseRowUtil.isAccumulateMsg(inputRow)) {
				windowAggregator.accumulate(inputRow);
			} else {
				windowAggregator.retract(inputRow);
			}
			acc = windowAggregator.getAccumulators();
			windowState.update(acc);
		}

		// the actual window which the input row is belongs to
		Collection<W> actualWindows = windowFunction.assignActualWindows(inputRow, timestamp);
		for (W window : actualWindows) {
			isElementDropped = false;
			triggerContext.window = window;
			boolean triggerResult = triggerContext.onElement(inputRow, timestamp);
			if (triggerResult) {
				emitWindowResult(window);
			}
			// register a clean up timer for the window
			registerCleanupTimer(window);
		}

		if (isElementDropped) {
			// markEvent will increase numLateRecordsDropped
			lateRecordsDroppedRate.markEvent();
		}
	}

	@Override
	public void onEventTime(InternalTimer<K, W> timer) throws Exception {
		setCurrentKey(timer.getKey());

		triggerContext.window = timer.getNamespace();
		if (triggerContext.onEventTime(timer.getTimestamp())) {
			// fire
			emitWindowResult(triggerContext.window);
		}

		if (windowAssigner.isEventTime()) {
			windowFunction.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		setCurrentKey(timer.getKey());

		triggerContext.window = timer.getNamespace();
		if (triggerContext.onProcessingTime(timer.getTimestamp())) {
			// fire
			emitWindowResult(triggerContext.window);
		}

		if (!windowAssigner.isEventTime()) {
			windowFunction.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
		}
	}

	/**
	 * Emits the window result of the given window.
	 */
	private void emitWindowResult(W window) throws Exception {
		BaseRow aggResult = windowFunction.getWindowAggregationResult(window);
		if (sendRetraction) {
			previousState.setCurrentNamespace(window);
			BaseRow previousAggResult = previousState.value();

			// has emitted result for the window
			if (previousAggResult != null) {
				// current agg is not equal to the previous emitted, should emit retract
				if (!equaliser.equalsWithoutHeader(aggResult, previousAggResult)) {
					reuseOutput.replace((BaseRow) getCurrentKey(), previousAggResult);
					BaseRowUtil.setRetract(reuseOutput);
					// send retraction
					collector.collect(reuseOutput);
					// send accumulate
					reuseOutput.replace((BaseRow) getCurrentKey(), aggResult);
					BaseRowUtil.setAccumulate(reuseOutput);
					collector.collect(reuseOutput);
					// update previousState
					previousState.update(aggResult);
				}
				// if the previous agg equals to the current agg, no need to send retract and accumulate
			}
			// the first fire for the window, only send accumulate
			else {
				// send accumulate
				reuseOutput.replace((BaseRow) getCurrentKey(), aggResult);
				BaseRowUtil.setAccumulate(reuseOutput);
				collector.collect(reuseOutput);
				// update previousState
				previousState.update(aggResult);
			}
		} else {
			reuseOutput.replace((BaseRow) getCurrentKey(), aggResult);
			// no need to set header
			collector.collect(reuseOutput);
		}
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
			long cleanupTime = Math.max(0, window.maxTimestamp() + allowedLateness);
			return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
		} else {
			return Math.max(0, window.maxTimestamp());
		}
	}

	@SuppressWarnings("unchecked")
	private K currentKey() {
		return (K) getCurrentKey();
	}

	// ------------------------------------------------------------------------------

	/**
	 * Context of window.
	 */
	private class WindowContext implements InternalWindowProcessFunction.Context<K, W> {

		@Override
		public <S extends State> S getPartitionedState(
				StateDescriptor<S, ?> stateDescriptor) throws Exception {
			requireNonNull(stateDescriptor, "The state properties must not be null");
			return WindowOperator.this.getPartitionedState(stateDescriptor);
		}

		@Override
		public K currentKey() {
			return WindowOperator.this.currentKey();
		}

		@Override
		public long currentProcessingTime() {
			return internalTimerService.currentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return internalTimerService.currentWatermark();
		}

		@Override
		public BaseRow getWindowAccumulators(W window) throws Exception {
			windowState.setCurrentNamespace(window);
			return windowState.value();
		}

		@Override
		public void setWindowAccumulators(W window, BaseRow acc) throws Exception {
			windowState.setCurrentNamespace(window);
			windowState.update(acc);
		}

		@Override
		public void clearWindowState(W window) throws Exception {
			windowState.setCurrentNamespace(window);
			windowState.clear();
		}

		@Override
		public void clearPreviousState(W window) throws Exception {
			if (previousState != null) {
				previousState.setCurrentNamespace(window);
				previousState.clear();
			}
		}

		@Override
		public void clearTrigger(W window) throws Exception {
			triggerContext.window = window;
			triggerContext.clear();
		}

		@Override
		public void deleteCleanupTimer(W window) throws Exception {
			long cleanupTime = cleanupTime(window);
			if (cleanupTime == Long.MAX_VALUE) {
				// no need to clean up because we didn't set one
				return;
			}
			if (windowAssigner.isEventTime()) {
				triggerContext.deleteEventTimeTimer(cleanupTime);
			} else {
				triggerContext.deleteProcessingTimeTimer(cleanupTime);
			}
		}

		@Override
		public void onMerge(W newWindow, Collection<W> mergedWindows) throws Exception {
			triggerContext.window = newWindow;
			triggerContext.mergedWindows = mergedWindows;
			triggerContext.onMerge();
		}
	}

	/**
	 * {@code TriggerContext} is a utility for handling {@code Trigger} invocations. It can be
	 * reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
	 * the {@code TriggerContext}
	 */
	private class TriggerContext implements Trigger.OnMergeContext {

		private W window;
		private Collection<W> mergedWindows;

		public void open() throws Exception {
			trigger.open(this);
		}

		boolean onElement(BaseRow row, long timestamp) throws Exception {
			return trigger.onElement(row, timestamp, window);
		}

		boolean onProcessingTime(long time) throws Exception {
			return trigger.onProcessingTime(time, window);
		}

		boolean onEventTime(long time) throws Exception {
			return trigger.onEventTime(time, window);
		}

		void onMerge() throws Exception {
			trigger.onMerge(window, this);
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
			return WindowOperator.this.getMetricGroup();
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

		public void clear() throws Exception {
			trigger.clear(window);
		}

		@Override
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return WindowOperator.this.getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}

		@Override
		public <S extends MergingState<?, ?>> void mergePartitionedState(
				StateDescriptor<S, ?> stateDescriptor) {
			if (mergedWindows != null && mergedWindows.size() > 0) {
				try {
					State state =
							WindowOperator.this.getOrCreateKeyedState(
									windowSerializer,
									stateDescriptor);
					if (state instanceof InternalMergingState) {
						((InternalMergingState<K, W, ?, ?, ?>) state).mergeNamespaces(window, mergedWindows);
					} else {
						throw new IllegalArgumentException(
								"The given state descriptor does not refer to a mergeable state (MergingState)");
					}
				}
				catch (Exception e) {
					throw new RuntimeException("Error while merging state.", e);
				}
			}
		}
	}


	// ------------------------------------------------------------------------------
	// Visible For Testing
	// ------------------------------------------------------------------------------

	protected Counter getNumLateRecordsDropped() {
		return numLateRecordsDropped;
	}

	protected Gauge<Long> getWatermarkLatency() {
		return watermarkLatency;
	}

}

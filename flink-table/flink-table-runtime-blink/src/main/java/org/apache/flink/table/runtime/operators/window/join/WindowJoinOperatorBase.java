/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.window.join;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.AbstractStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.internal.InternalWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.internal.WindowJoinProcessFunction;
import org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateView;
import org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateViews;
import org.apache.flink.table.runtime.operators.window.join.state.WindowedStateView;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for window-join operators.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public abstract class WindowJoinOperatorBase<K, W extends Window>
		extends AbstractStreamingJoinOperator
		implements Triggerable<K, W> {

	private static final long serialVersionUID = 1L;

	private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
	private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
	private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

	// -------------------------------------------------------------------------
	//  Window Configuration
	// -------------------------------------------------------------------------

	protected final WindowAssigner<W> leftAssigner;

	protected final WindowAssigner<W> rightAssigner;

	private final Trigger<W> trigger;

	/** For serializing the window in checkpoints. */
	protected final TypeSerializer<W> windowSerializer;

	/**
	 * The allowed lateness for elements. This is used for:
	 * <ul>
	 * <li>Deciding if an element should be dropped from a window due to lateness.
	 * <li>Clearing the state of a window if the system time passes the
	 * {@code window.maxTimestamp + allowedLateness} landmark.
	 * </ul>
	 */
	protected final long allowedLateness;

	/** Process function for first input. **/
	protected transient WindowJoinProcessFunction<K, W> windowFunction1;

	/** Process function for second input. **/
	protected transient WindowJoinProcessFunction<K, W> windowFunction2;

	protected transient InternalTimerService<W> internalTimerService;

	private transient TriggerContext triggerContext;

	// -------------------------------------------------------------------------
	//  Join Configuration
	// -------------------------------------------------------------------------

	// whether left hand side can generate nulls
	private final boolean generateNullsOnLeft;
	// whether right hand side can generate nulls
	private final boolean generateNullsOnRight;

	private final WindowAttribute leftWindowAttr;
	private final WindowAttribute rightWindowAttr;
	// used to output left data row and window attributes
	private transient WindowAttrCollectors.WindowAttrCollector leftCollector;
	// used to output right data row and window attributes
	private transient WindowAttrCollectors.WindowAttrCollector rightCollector;

	private transient RowData leftNullRow;
	private transient RowData rightNullRow;
	// output row: data row + window attributes
	private transient JoinedRowData outputRow;

	// left join state
	protected transient WindowJoinRecordStateView<W> joinInputView1;
	// right join state
	protected transient WindowJoinRecordStateView<W> joinInputView2;

	// ------------------------------------------------------------------------
	// Metrics
	// ------------------------------------------------------------------------

	private transient Counter numLateRecordsDropped;
	private transient Meter lateRecordsDroppedRate;
	private transient Gauge<Long> watermarkLatency;

	protected WindowJoinOperatorBase(
			WindowAssigner<W> leftAssigner,
			WindowAssigner<W> rightAssigner,
			Trigger<W> trigger,
			TypeSerializer<W> windowSerializer,
			long allowedLateness,
			InternalTypeInfo<RowData> leftType,
			InternalTypeInfo<RowData> rightType,
			GeneratedJoinCondition generatedJoinCondition,
			JoinInputSideSpec leftInputSideSpec,
			JoinInputSideSpec rightInputSideSpec,
			boolean generateNullsOnLeft,
			boolean generateNullsOnRight,
			WindowAttribute leftWindowAttr,
			WindowAttribute rightWindowAttr,
			boolean[] filterNullKeys) {
		super(leftType, rightType, generatedJoinCondition, leftInputSideSpec,
				rightInputSideSpec, filterNullKeys, -1);
		this.leftAssigner = checkNotNull(leftAssigner);
		this.rightAssigner = checkNotNull(rightAssigner);
		this.trigger = checkNotNull(trigger);
		this.windowSerializer = checkNotNull(windowSerializer);
		checkArgument(allowedLateness >= 0);
		this.allowedLateness = allowedLateness;
		this.generateNullsOnLeft = generateNullsOnLeft;
		this.generateNullsOnRight = generateNullsOnRight;
		this.leftWindowAttr = checkNotNull(leftWindowAttr);
		this.rightWindowAttr = checkNotNull(rightWindowAttr);
	}

	@Override
	public void open() throws Exception {
		super.open();

		internalTimerService = getInternalTimerService("window-join-timers",
				windowSerializer, this);

		triggerContext = new TriggerContext();
		triggerContext.open();

		this.outputRow = new JoinedRowData();
		this.leftNullRow = new GenericRowData(leftType.toRowType().getFieldCount());
		this.rightNullRow = new GenericRowData(rightType.toRowType().getFieldCount());
		this.leftCollector = WindowAttrCollectors.getWindowAttrCollector(leftWindowAttr);
		this.rightCollector = WindowAttrCollectors.getWindowAttrCollector(rightWindowAttr);
		ViewContext viewContext = new ViewContext();
		// initialize states
		this.joinInputView1 = WindowJoinRecordStateViews.create(
				viewContext,
				windowSerializer,
				"left-records",
				leftInputSideSpec,
				leftType);

		this.joinInputView2 = WindowJoinRecordStateViews.create(
				viewContext,
				windowSerializer,
				"right-records",
				rightInputSideSpec,
				rightType);

		WindowContext windowContext1 = new WindowContext(this.joinInputView1);
		WindowContext windowContext2 = new WindowContext(this.joinInputView2);
		initializeProcessFunction();
		this.windowFunction1.open(windowContext1);
		this.windowFunction2.open(windowContext2);

		// metrics
		numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
		this.lateRecordsDroppedRate = metrics.meter(
				LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
				new MeterView(numLateRecordsDropped));
		this.watermarkLatency = metrics.gauge(WATERMARK_LATENCY_METRIC_NAME, () -> {
			long watermark = internalTimerService.currentWatermark();
			if (watermark < 0) {
				return 0L;
			} else {
				return internalTimerService.currentProcessingTime() - watermark;
			}
		});
	}

	/**
	 * Initialize the window process function. This function is invoked in the {@link #open()}.
	 *
	 * @see #open()
	 */
	protected abstract void initializeProcessFunction();

	@Override
	public void close() throws Exception {
		super.close();
		collector = null;
		triggerContext = null;
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		collector = null;
		triggerContext = null;
	}

	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {
		processElement(element, true);
	}

	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {
		processElement(element, false);
	}

	/**
	 * Get the timestamp of the record, this method is invoked
	 * in the {@link #processElement(StreamRecord, boolean)}.
	 *
	 * @see #processElement(StreamRecord, boolean)
	 */
	protected abstract long getRecordTimestamp(RowData inputRow, boolean isLeft);

	private void processElement (StreamRecord<RowData> element, boolean isLeft)
			throws Exception {
		final InternalWindowProcessFunction<K, W> windowFunction = isLeft ? windowFunction1 : windowFunction2;
		final WindowJoinRecordStateView<W> joinInputView = isLeft ? joinInputView1 : joinInputView2;
		RowData inputRow = element.getValue();
		long timestamp = getRecordTimestamp(inputRow, isLeft);

		// the windows which the input row should be placed into
		Collection<W> stateWindows = windowFunction.assignStateNamespace(inputRow, timestamp);
		boolean isElementDropped = true;
		for (W window : stateWindows) {
			isElementDropped = false;
			joinInputView.setCurrentNamespace(window);
			if (inputRow.getRowKind() == RowKind.UPDATE_BEFORE) {
				// Ignore update before message.
				continue;
			}
			if (inputRow.getRowKind() == RowKind.DELETE) {
				// Erase RowKind for state updating
				inputRow.setRowKind(RowKind.INSERT);
				joinInputView.retractRecord(inputRow);
			} else {
				joinInputView.addRecord(inputRow);
			}
		}

		// the actual window which the input row is belongs to
		Collection<W> actualWindows = windowFunction.assignActualWindows(inputRow, timestamp);
		for (W window : actualWindows) {
			isElementDropped = false;
			triggerContext.window = window;
			boolean triggerResult = triggerContext.onElement(inputRow, timestamp);
			if (triggerResult) {
				joinWindowInputsAndEmit(window);
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
			joinWindowInputsAndEmit(triggerContext.window);
		}

		if (leftAssigner.isEventTime()) {
			windowFunction1.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
			windowFunction2.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		setCurrentKey(timer.getKey());

		triggerContext.window = timer.getNamespace();
		if (triggerContext.onProcessingTime(timer.getTimestamp())) {
			// fire
			joinWindowInputsAndEmit(triggerContext.window);
		}

		if (!isEventTime()) {
			windowFunction1.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
			windowFunction2.cleanWindowIfNeeded(triggerContext.window, timer.getTimestamp());
		}
	}

	// -------------------------------------------------------------------------
	//  Inner Class
	// -------------------------------------------------------------------------

	/**
	 * Context of window.
	 */
	private class WindowContext implements InternalWindowProcessFunction.Context<K, W> {
		private WindowJoinRecordStateView<W> view;

		WindowContext(WindowJoinRecordStateView<W> view) {
			this.view = view;
		}

		@Override
		public <S extends State> S getPartitionedState(
				StateDescriptor<S, ?> stateDescriptor) throws Exception {
			requireNonNull(stateDescriptor, "The state properties must not be null");
			return WindowJoinOperatorBase.this.getPartitionedState(stateDescriptor);
		}

		@Override
		public K currentKey() {
			return WindowJoinOperatorBase.this.currentKey();
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
		public void clearWindowState(W window) {
			this.view.setCurrentNamespace(window);
			this.view.clear();
		}

		@Override
		public void clearTrigger(W window) throws Exception {
			triggerContext.window = window;
			triggerContext.clear();
		}

		@Override
		public void deleteCleanupTimer(W window) {
			long cleanupTime = cleanupTime(window);
			if (cleanupTime == Long.MAX_VALUE) {
				// no need to clean up because we didn't set one
				return;
			}
			if (isEventTime()) {
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
	 * reused by setting the {@code key} and {@code window} fields.
	 * Non-internal state must be kept in the {@code TriggerContext}.
	 */
	private class TriggerContext implements Trigger.OnMergeContext {

		private W window;
		private Collection<W> mergedWindows;

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
			return WindowJoinOperatorBase.this.getMetricGroup();
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
				return WindowJoinOperatorBase.this.getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public <S extends MergingState<?, ?>> void mergePartitionedState(
				StateDescriptor<S, ?> stateDescriptor) {
			if (mergedWindows != null && mergedWindows.size() > 0) {
				try {
					State state =
							WindowJoinOperatorBase.this.getOrCreateKeyedState(
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

	/** Context of {@code WindowedStateView}. */
	private class ViewContext implements WindowedStateView.Context {
		@Override
		public <S extends State, N extends Window> S getOrCreateKeyedState(
				TypeSerializer<N> windowSerializer,
				StateDescriptor<S, ?> stateDescriptor) throws Exception {
			return WindowJoinOperatorBase.this.getOrCreateKeyedState(windowSerializer, stateDescriptor);
		}
	}

	// -------------------------------------------------------------------------
	//  Utilities
	// -------------------------------------------------------------------------

	/**
	 * Emits the window result of the given window.
	 */
	private void joinWindowInputsAndEmit(W window) throws Exception {
		Iterable<RowData> leftInputs = this.windowFunction1.prepareInputsToJoin(window);
		Iterable<RowData> rightInputs = this.windowFunction2.prepareInputsToJoin(window);
		if (generateNullsOnLeft) {
			joinLeftIsOuter(window, leftInputs, rightInputs, generateNullsOnRight);
		} else {
			joinLeftNonOuter(window, leftInputs, rightInputs, generateNullsOnRight);
		}
	}

	private void joinLeftNonOuter(
			W window,
			Iterable<RowData> leftInputs,
			Iterable<RowData> rightInputs,
			boolean rightGeneratesNulls) {
		for (RowData left : leftInputs) {
			boolean leftMatched = false;
			for (RowData right : rightInputs) {
				boolean matches = joinCondition.apply(left, right);
				if (matches) {
					leftMatched = true;
					output(left, right, window);
				}
			}
			if (!leftMatched && rightGeneratesNulls) {
				outputNullPadding(left, true, window);
			}
		}
	}

	private void joinLeftIsOuter(
			W window,
			Iterable<RowData> leftInputs,
			Iterable<RowData> rightInputs,
			boolean rightGeneratesNulls) {
		Set<Integer> rightMatchedIndices = new HashSet<>();
		for (RowData left : leftInputs) {
			boolean leftMatched = false;
			int idx = 0;
			for (RowData right : rightInputs) {
				boolean matches = joinCondition.apply(left, right);
				if (matches) {
					leftMatched = true;
					output(left, right, window);
					rightMatchedIndices.add(idx);
				}
				idx++;
			}
			if (!leftMatched && rightGeneratesNulls) {
				outputNullPadding(left, true, window);
			}
		}
		int idx = 0;
		for (RowData right : rightInputs) {
			if (!rightMatchedIndices.contains(idx)) {
				outputNullPadding(right, false, window);
			}
			idx++;
		}
	}

	/** Returns whether the time-attribute is event-time. */
	protected boolean isEventTime() {
		return this.leftAssigner.isEventTime();
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

		if (isEventTime()) {
			triggerContext.registerEventTimeTimer(cleanupTime);
		} else {
			triggerContext.registerProcessingTimeTimer(cleanupTime);
		}
	}

	/**
	 * Returns the cleanup time for a window, which is
	 * {@code window.maxTimestamp + allowedLateness}. In
	 * case this leads to a value greater than {@link Long#MAX_VALUE}
	 * then a cleanup time of {@link Long#MAX_VALUE} is
	 * returned.
	 *
	 * @param window the window whose cleanup time we are computing.
	 */
	private long cleanupTime(W window) {
		if (isEventTime()) {
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

	private void output(RowData inputRow, RowData otherRow, W window) {
		RowData left = leftCollector.collect(inputRow, (TimeWindow) window);
		RowData right = rightCollector.collect(otherRow, (TimeWindow) window);

		outputRow.replace(left, right);
		collector.collect(outputRow);
	}

	private void outputNullPadding(RowData row, boolean isLeft, W window) {
		RowData leftData;
		RowData rightData;
		if (isLeft) {
			leftData = leftCollector.collect(row, (TimeWindow) window);
			rightData = rightCollector.collect(rightNullRow, (TimeWindow) window);
		} else {
			leftData = leftCollector.collect(leftNullRow, (TimeWindow) window);
			rightData = rightCollector.collect(row, (TimeWindow) window);
		}
		outputRow.replace(leftData, rightData);
		collector.collect(outputRow);
	}

	// ------------------------------------------------------------------------------
	// Visible For Testing
	// ------------------------------------------------------------------------------

	@VisibleForTesting
	protected Counter getNumLateRecordsDropped() {
		return numLateRecordsDropped;
	}

	@VisibleForTesting
	protected Gauge<Long> getWatermarkLatency() {
		return watermarkLatency;
	}
}

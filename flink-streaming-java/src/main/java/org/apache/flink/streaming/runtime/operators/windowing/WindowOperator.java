/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.datastream.LegacyWindowOperatorType;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that implements the logic for windowing based on a {@link WindowAssigner} and
 * {@link Trigger}.
 *
 * <p>
 * When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes if it was assigned to multiple windows by the
 * {@code WindowAssigner}.
 *
 * <p>
 * Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires,
 * the given {@link InternalWindowFunction} is invoked to produce the results that are emitted for
 * the pane to which the {@code Trigger} belongs.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class WindowOperator<K, IN, ACC, OUT, W extends Window>
	extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
	implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	protected final WindowAssigner<? super IN, W> windowAssigner;

	protected final KeySelector<IN, K> keySelector;

	protected final Trigger<? super IN, ? super W> trigger;

	protected final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;

	protected final ListStateDescriptor<Tuple2<W, W>> mergingWindowsDescriptor;

	/**
	 * For serializing the key in checkpoints.
	 */
	protected final TypeSerializer<K> keySerializer;

	/**
	 * For serializing the window in checkpoints.
	 */
	protected final TypeSerializer<W> windowSerializer;

	/**
	 * The allowed lateness for elements. This is used for:
	 * <ul>
	 *     <li>Deciding if an element should be dropped from a window due to lateness.
	 *     <li>Clearing the state of a window if the system time passes the
	 *         {@code window.maxTimestamp + allowedLateness} landmark.
	 * </ul>
	 */
	protected final long allowedLateness;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/**
	 * This is given to the {@code InternalWindowFunction} for emitting elements with a given timestamp.
	 */
	protected transient TimestampedCollector<OUT> timestampedCollector;

	protected transient Context context = new Context(null, null);

	protected transient WindowAssigner.WindowAssignerContext windowAssignerContext;

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	protected transient InternalTimerService<W> internalTimerService;

	// ------------------------------------------------------------------------
	// State restored in case of migration from an older version (backwards compatibility)
	// ------------------------------------------------------------------------

	/**
	 * A flag indicating if we are migrating from a regular {@link WindowOperator}
	 * or one of the deprecated {@link AccumulatingProcessingTimeWindowOperator} and
	 * {@link AggregatingProcessingTimeWindowOperator}.
	 */
	private final LegacyWindowOperatorType legacyWindowOperatorType;

	/**
	 * The elements restored when migrating from an older, deprecated
	 * {@link AccumulatingProcessingTimeWindowOperator} or
	 * {@link AggregatingProcessingTimeWindowOperator}. */
	private transient PriorityQueue<StreamRecord<IN>> restoredFromLegacyAlignedOpRecords;

	/**
	 * The restored processing time timers when migrating from an
	 * older version of the {@link WindowOperator}.
	 */
	private transient PriorityQueue<Timer<K, W>> restoredFromLegacyProcessingTimeTimers;

	/** The restored event time timer when migrating from an
	 * older version of the {@link WindowOperator}.
	 */
	private transient PriorityQueue<Timer<K, W>> restoredFromLegacyEventTimeTimers;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public WindowOperator(
			WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			KeySelector<IN, K> keySelector,
			TypeSerializer<K> keySerializer,
			StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
			InternalWindowFunction<ACC, OUT, K, W> windowFunction,
			Trigger<? super IN, ? super W> trigger,
			long allowedLateness) {

		this(windowAssigner, windowSerializer, keySelector, keySerializer,
			windowStateDescriptor, windowFunction, trigger, allowedLateness, LegacyWindowOperatorType.NONE);
	}

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public WindowOperator(
			WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			KeySelector<IN, K> keySelector,
			TypeSerializer<K> keySerializer,
			StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
			InternalWindowFunction<ACC, OUT, K, W> windowFunction,
			Trigger<? super IN, ? super W> trigger,
			long allowedLateness,
			LegacyWindowOperatorType legacyWindowOperatorType) {

		super(windowFunction);

		checkArgument(!(windowAssigner instanceof BaseAlignedWindowAssigner),
			"The " + windowAssigner.getClass().getSimpleName() + " cannot be used with a WindowOperator. " +
				"This assigner is only used with the AccumulatingProcessingTimeWindowOperator and " +
				"the AggregatingProcessingTimeWindowOperator");

		checkArgument(allowedLateness >= 0);

		checkArgument(windowStateDescriptor == null || windowStateDescriptor.isSerializerInitialized(),
				"window state serializer is not properly initialized");

		this.windowAssigner = checkNotNull(windowAssigner);
		this.windowSerializer = checkNotNull(windowSerializer);
		this.keySelector = checkNotNull(keySelector);
		this.keySerializer = checkNotNull(keySerializer);
		this.windowStateDescriptor = windowStateDescriptor;
		this.trigger = checkNotNull(trigger);
		this.allowedLateness = allowedLateness;
		this.legacyWindowOperatorType = legacyWindowOperatorType;

		if (windowAssigner instanceof MergingWindowAssigner) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>((Class) Tuple2.class, new TypeSerializer[] {windowSerializer, windowSerializer} );
			mergingWindowsDescriptor = new ListStateDescriptor<>("merging-window-set", tupleSerializer);
		} else {
			mergingWindowsDescriptor = null;
		}

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void open() throws Exception {
		super.open();

		timestampedCollector = new TimestampedCollector<>(output);

		internalTimerService =
				getInternalTimerService("window-timers", windowSerializer, this);

		context = new Context(null, null);

		windowAssignerContext = new WindowAssigner.WindowAssignerContext() {
			@Override
			public long getCurrentProcessingTime() {
				return internalTimerService.currentProcessingTime();
			}
		};

		registerRestoredLegacyStateState();
	}

	@Override
	public void close() throws Exception {
		super.close();
		timestampedCollector = null;
		context = null;
		windowAssignerContext = null;
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		timestampedCollector = null;
		context = null;
		windowAssignerContext = null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamRecord<IN> element) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		final K key = (K) getKeyedStateBackend().getCurrentKey();

		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window: elementWindows) {

				// adding the new window might result in a merge, in that case the actualWindow
				// is the merged window and we work with that. If we don't merge then
				// actualWindow == window
				W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
					@Override
					public void merge(W mergeResult,
							Collection<W> mergedWindows, W stateWindowResult,
							Collection<W> mergedStateWindows) throws Exception {
						context.key = key;
						context.window = mergeResult;

						context.onMerge(mergedWindows);

						for (W m: mergedWindows) {
							context.window = m;
							context.clear();
							deleteCleanupTimer(m);
						}

						// merge the merged state windows into the newly resulting state window
						getKeyedStateBackend().mergePartitionedStates(
							stateWindowResult,
							mergedStateWindows,
							windowSerializer,
							(StateDescriptor<? extends MergingState<?,?>, ?>) windowStateDescriptor);
					}
				});

				// drop if the window is already late
				if (isLate(actualWindow)) {
					mergingWindows.retireWindow(actualWindow);
					continue;
				}

				W stateWindow = mergingWindows.getStateWindow(actualWindow);
				if (stateWindow == null) {
					throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
				}

				AppendingState<IN, ACC> windowState =
						getPartitionedState(stateWindow, windowSerializer, windowStateDescriptor);
				windowState.add(element.getValue());

				context.key = key;
				context.window = actualWindow;

				TriggerResult triggerResult = context.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(actualWindow, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(actualWindow);
			}

			// need to make sure to update the merging state in state
			mergingWindows.persist();
		} else {
			for (W window: elementWindows) {

				// drop if the window is already late
				if (isLate(window)) {
					continue;
				}

				AppendingState<IN, ACC> windowState =
						getPartitionedState(window, windowSerializer, windowStateDescriptor);
				windowState.add(element.getValue());

				context.key = key;
				context.window = window;

				TriggerResult triggerResult = context.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(window, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(window);
			}
		}
	}

	@Override
	public void onEventTime(InternalTimer<K, W> timer) throws Exception {
		context.key = timer.getKey();
		context.window = timer.getNamespace();

		AppendingState<IN, ACC> windowState;
		MergingWindowSet<W> mergingWindows = null;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(context.window);
			if (stateWindow == null) {
				// timer firing for non-existent window, ignore
				windowState = null;
			} else {
				windowState = getPartitionedState(
						stateWindow,
						windowSerializer,
						windowStateDescriptor);
			}
		} else {
			windowState = getPartitionedState(context.window, windowSerializer, windowStateDescriptor);
		}

		ACC contents = null;
		if (windowState != null) {
			contents = windowState.get();
		}

		if (contents != null) {
			TriggerResult triggerResult = context.onEventTime(timer.getTimestamp());
			if (triggerResult.isFire()) {
				emitWindowContents(context.window, contents);
			}
			if (triggerResult.isPurge()) {
				windowState.clear();
			}
		}

		if (windowAssigner.isEventTime() && isCleanupTime(context.window, timer.getTimestamp())) {
			clearAllState(context.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		context.key = timer.getKey();
		context.window = timer.getNamespace();

		AppendingState<IN, ACC> windowState;
		MergingWindowSet<W> mergingWindows = null;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(context.window);
			if (stateWindow == null) {
				// timer firing for non-existent window, ignore
				windowState = null;
			} else {
				windowState = getPartitionedState(
						stateWindow,
						windowSerializer,
						windowStateDescriptor);
			}
		} else {
			windowState = getPartitionedState(context.window, windowSerializer, windowStateDescriptor);
		}

		ACC contents = null;
		if (windowState != null) {
			contents = windowState.get();
		}

		if (contents != null) {
			TriggerResult triggerResult = context.onProcessingTime(timer.getTimestamp());
			if (triggerResult.isFire()) {
				emitWindowContents(context.window, contents);
			}
			if (triggerResult.isPurge()) {
				windowState.clear();
			}
		}

		if (!windowAssigner.isEventTime() && isCleanupTime(context.window, timer.getTimestamp())) {
			clearAllState(context.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	/**
	 * Drops all state for the given window and calls
	 * {@link Trigger#clear(Window, Trigger.TriggerContext)}.
	 *
	 * <p>The caller must ensure that the
	 * correct key is set in the state backend and the context object.
	 */
	private void clearAllState(
			W window,
			AppendingState<IN, ACC> windowState,
			MergingWindowSet<W> mergingWindows) throws Exception {
		windowState.clear();
		context.clear();
		if (mergingWindows != null) {
			mergingWindows.retireWindow(window);
			mergingWindows.persist();
		}
	}

	/**
	 * Emits the contents of the given window using the {@link InternalWindowFunction}.
	 */
	@SuppressWarnings("unchecked")
	private void emitWindowContents(W window, ACC contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
		userFunction.apply(context.key, context.window, contents, timestampedCollector);
	}

	/**
	 * Retrieves the {@link MergingWindowSet} for the currently active key.
	 * The caller must ensure that the correct key is set in the state backend.
	 *
	 * <p>The caller must also ensure to properly persist changes to state using
	 * {@link MergingWindowSet#persist()}.
	 */
	protected MergingWindowSet<W> getMergingWindowSet() throws Exception {
		ListState<Tuple2<W, W>> mergeState =
				getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, mergingWindowsDescriptor);

		@SuppressWarnings({"unchecked", "rawtypes"})
		MergingWindowSet<W> mergingWindows = new MergingWindowSet<>((MergingWindowAssigner) windowAssigner, mergeState);

		return mergingWindows;
	}

	/**
	 * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness
	 * of the given window.
	 */
	protected boolean isLate(W window) {
		return (windowAssigner.isEventTime() && (cleanupTime(window) <= internalTimerService.currentWatermark()));
	}

	/**
	 * Registers a timer to cleanup the content of the window.
	 * @param window
	 * 					the window whose state to discard
	 */
	protected void registerCleanupTimer(W window) {
		long cleanupTime = cleanupTime(window);
		if (cleanupTime == Long.MAX_VALUE) {
			// don't set a GC timer for "end of time"
			return;
		}

		if (windowAssigner.isEventTime()) {
			context.registerEventTimeTimer(cleanupTime);
		} else {
			context.registerProcessingTimeTimer(cleanupTime);
		}
	}

	/**
	 * Deletes the cleanup timer set for the contents of the provided window.
	 * @param window
	 * 					the window whose state to discard
	 */
	protected void deleteCleanupTimer(W window) {
		long cleanupTime = cleanupTime(window);
		if (cleanupTime == Long.MAX_VALUE) {
			// no need to clean up because we didn't set one
			return;
		}
		if (windowAssigner.isEventTime()) {
			context.deleteEventTimeTimer(cleanupTime);
		} else {
			context.deleteProcessingTimeTimer(cleanupTime);
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
			long cleanupTime = window.maxTimestamp() + allowedLateness;
			return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
		} else {
			return window.maxTimestamp();
		}
	}

	/**
	 * Returns {@code true} if the given time is the cleanup time for the given window.
	 */
	protected final boolean isCleanupTime(W window, long time) {
		return time == cleanupTime(window);
	}

	/**
	 * {@code Context} is a utility for handling {@code Trigger} invocations. It can be reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
	 * the {@code Context}
	 */
	public class Context implements Trigger.OnMergeContext {
		protected K key;
		protected W window;

		protected Collection<W> mergedWindows;

		public Context(K key, W window) {
			this.key = key;
			this.window = window;
		}

		@Override
		public MetricGroup getMetricGroup() {
			return WindowOperator.this.getMetricGroup();
		}

		public long getCurrentWatermark() {
			return internalTimerService.currentWatermark();
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			Class<S> stateType,
			S defaultState) {
			checkNotNull(stateType, "The state type class must not be null");

			TypeInformation<S> typeInfo;
			try {
				typeInfo = TypeExtractor.getForClass(stateType);
			}
			catch (Exception e) {
				throw new RuntimeException("Cannot analyze type '" + stateType.getName() +
					"' from the class alone, due to generic type parameters. " +
					"Please specify the TypeInformation directly.", e);
			}

			return getKeyValueState(name, typeInfo, defaultState);
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			TypeInformation<S> stateType,
			S defaultState) {

			checkNotNull(name, "The name of the state must not be null");
			checkNotNull(stateType, "The state type information must not be null");

			ValueStateDescriptor<S> stateDesc = new ValueStateDescriptor<>(name, stateType.createSerializer(getExecutionConfig()), defaultState);
			return getPartitionedState(stateDesc);
		}

		@SuppressWarnings("unchecked")
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return WindowOperator.this.getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}

		@Override
		public <S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			if (mergedWindows != null && mergedWindows.size() > 0) {
				try {
					WindowOperator.this.getKeyedStateBackend().mergePartitionedStates(window,
							mergedWindows,
							windowSerializer,
							stateDescriptor);
				} catch (Exception e) {
					throw new RuntimeException("Error while merging state.", e);
				}
			}
		}

		@Override
		public long getCurrentProcessingTime() {
			return internalTimerService.currentProcessingTime();
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

		public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
			return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
		}

		public TriggerResult onProcessingTime(long time) throws Exception {
			return trigger.onProcessingTime(time, window, this);
		}

		public TriggerResult onEventTime(long time) throws Exception {
			return trigger.onEventTime(time, window, this);
		}

		public void onMerge(Collection<W> mergedWindows) throws Exception {
			this.mergedWindows = mergedWindows;
			trigger.onMerge(window, this);
		}

		public void clear() throws Exception {
			trigger.clear(window, this);
		}

		@Override
		public String toString() {
			return "Context{" +
				"key=" + key +
				", window=" + window +
				'}';
		}
	}

	/**
	 * Internal class for keeping track of in-flight timers.
	 */
	protected static class Timer<K, W extends Window> implements Comparable<Timer<K, W>> {
		protected long timestamp;
		protected K key;
		protected W window;

		public Timer(long timestamp, K key, W window) {
			this.timestamp = timestamp;
			this.key = key;
			this.window = window;
		}

		@Override
		public int compareTo(Timer<K, W> o) {
			return Long.compare(this.timestamp, o.timestamp);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()){
				return false;
			}

			Timer<?, ?> timer = (Timer<?, ?>) o;

			return timestamp == timer.timestamp
				&& key.equals(timer.key)
				&& window.equals(timer.window);

		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + key.hashCode();
			result = 31 * result + window.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Timer{" +
				"timestamp=" + timestamp +
				", key=" + key +
				", window=" + window +
				'}';
		}
	}

	// ------------------------------------------------------------------------
	//  Restoring / Migrating from an older Flink version.
	// ------------------------------------------------------------------------

	private static final int BEGIN_OF_STATE_MAGIC_NUMBER = 0x0FF1CE42;

	private static final int BEGIN_OF_PANE_MAGIC_NUMBER = 0xBADF00D5;

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		super.restoreState(in);

		LOG.info("{} (taskIdx={}) restoring {} state from an older Flink version.",
			getClass().getSimpleName(), legacyWindowOperatorType, getRuntimeContext().getIndexOfThisSubtask());

		DataInputViewStreamWrapper streamWrapper = new DataInputViewStreamWrapper(in);

		switch (legacyWindowOperatorType) {
			case NONE:
				restoreFromLegacyWindowOperator(streamWrapper);
				break;
			case FAST_ACCUMULATING:
			case FAST_AGGREGATING:
				restoreFromLegacyAlignedWindowOperator(streamWrapper);
				break;
		}
	}

	public void registerRestoredLegacyStateState() throws Exception {

		switch (legacyWindowOperatorType) {
			case NONE:
				reregisterStateFromLegacyWindowOperator();
				break;
			case FAST_ACCUMULATING:
			case FAST_AGGREGATING:
				reregisterStateFromLegacyAlignedWindowOperator();
				break;
		}
	}

	private void restoreFromLegacyAlignedWindowOperator(DataInputViewStreamWrapper in) throws IOException {
		Preconditions.checkArgument(legacyWindowOperatorType != LegacyWindowOperatorType.NONE);

		final long nextEvaluationTime = in.readLong();
		final long nextSlideTime = in.readLong();

		validateMagicNumber(BEGIN_OF_STATE_MAGIC_NUMBER, in.readInt());

		restoredFromLegacyAlignedOpRecords = new PriorityQueue<>(42,
			new Comparator<StreamRecord<IN>>() {
				@Override
				public int compare(StreamRecord<IN> o1, StreamRecord<IN> o2) {
					return Long.compare(o1.getTimestamp(), o2.getTimestamp());
				}
			}
		);

		switch (legacyWindowOperatorType) {
			case FAST_ACCUMULATING:
				restoreElementsFromLegacyAccumulatingAlignedWindowOperator(in, nextSlideTime);
				break;
			case FAST_AGGREGATING:
				restoreElementsFromLegacyAggregatingAlignedWindowOperator(in, nextSlideTime);
				break;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} (taskIdx={}) restored {} events from legacy {}.",
				getClass().getSimpleName(),
				getRuntimeContext().getIndexOfThisSubtask(),
				restoredFromLegacyAlignedOpRecords.size(),
				legacyWindowOperatorType);
		}
	}

	private void restoreElementsFromLegacyAccumulatingAlignedWindowOperator(DataInputView in, long nextSlideTime) throws IOException {
		int numPanes = in.readInt();
		final long paneSize = getPaneSize();
		long nextElementTimestamp = nextSlideTime - (numPanes * paneSize);

		@SuppressWarnings("unchecked")
		ArrayListSerializer<IN> ser = new ArrayListSerializer<>((TypeSerializer<IN>) getStateDescriptor().getSerializer());

		while (numPanes > 0) {
			validateMagicNumber(BEGIN_OF_PANE_MAGIC_NUMBER, in.readInt());

			nextElementTimestamp += paneSize - 1; // the -1 is so that the elements fall into the correct time-frame

			final int numElementsInPane = in.readInt();
			for (int i = numElementsInPane - 1; i >= 0; i--) {
				K key = keySerializer.deserialize(in);

				@SuppressWarnings("unchecked")
				List<IN> valueList = ser.deserialize(in);
				for (IN record: valueList) {
					restoredFromLegacyAlignedOpRecords.add(new StreamRecord<>(record, nextElementTimestamp));
				}
			}
			numPanes--;
		}
	}

	private void restoreElementsFromLegacyAggregatingAlignedWindowOperator(DataInputView in, long nextSlideTime) throws IOException {
		int numPanes = in.readInt();
		final long paneSize = getPaneSize();
		long nextElementTimestamp = nextSlideTime - (numPanes * paneSize);

		while (numPanes > 0) {
			validateMagicNumber(BEGIN_OF_PANE_MAGIC_NUMBER, in.readInt());

			nextElementTimestamp += paneSize - 1; // the -1 is so that the elements fall into the correct time-frame

			final int numElementsInPane = in.readInt();
			for (int i = numElementsInPane - 1; i >= 0; i--) {
				K key = keySerializer.deserialize(in);

				@SuppressWarnings("unchecked")
				IN value = (IN) getStateDescriptor().getSerializer().deserialize(in);
				restoredFromLegacyAlignedOpRecords.add(new StreamRecord<>(value, nextElementTimestamp));
			}
			numPanes--;
		}
	}

	private long getPaneSize() {
		Preconditions.checkArgument(
			legacyWindowOperatorType == LegacyWindowOperatorType.FAST_ACCUMULATING ||
				legacyWindowOperatorType == LegacyWindowOperatorType.FAST_AGGREGATING);

		final long paneSlide;
		if (windowAssigner instanceof SlidingProcessingTimeWindows) {
			SlidingProcessingTimeWindows timeWindows = (SlidingProcessingTimeWindows) windowAssigner;
			paneSlide = ArithmeticUtils.gcd(timeWindows.getSize(), timeWindows.getSlide());
		} else {
			TumblingProcessingTimeWindows timeWindows = (TumblingProcessingTimeWindows) windowAssigner;
			paneSlide = timeWindows.getSize(); // this is valid as windowLength == windowSlide == timeWindows.getSize
		}
		return paneSlide;
	}

	private static void validateMagicNumber(int expected, int found) throws IOException {
		if (expected != found) {
			throw new IOException("Corrupt state stream - wrong magic number. " +
				"Expected '" + Integer.toHexString(expected) +
				"', found '" + Integer.toHexString(found) + '\'');
		}
	}

	private void restoreFromLegacyWindowOperator(DataInputViewStreamWrapper in) throws IOException {
		Preconditions.checkArgument(legacyWindowOperatorType == LegacyWindowOperatorType.NONE);

		int numWatermarkTimers = in.readInt();
		this.restoredFromLegacyEventTimeTimers = new PriorityQueue<>(Math.max(numWatermarkTimers, 1));

		for (int i = 0; i < numWatermarkTimers; i++) {
			K key = keySerializer.deserialize(in);
			W window = windowSerializer.deserialize(in);
			long timestamp = in.readLong();

			Timer<K, W> timer = new Timer<>(timestamp, key, window);
			restoredFromLegacyEventTimeTimers.add(timer);
		}

		int numProcessingTimeTimers = in.readInt();
		this.restoredFromLegacyProcessingTimeTimers = new PriorityQueue<>(Math.max(numProcessingTimeTimers, 1));

		for (int i = 0; i < numProcessingTimeTimers; i++) {
			K key = keySerializer.deserialize(in);
			W window = windowSerializer.deserialize(in);
			long timestamp = in.readLong();

			Timer<K, W> timer = new Timer<>(timestamp, key, window);
			restoredFromLegacyProcessingTimeTimers.add(timer);
		}

		// just to read all the rest, although we do not really use this information.
		int numProcessingTimeTimerTimestamp = in.readInt();
		for (int i = 0; i< numProcessingTimeTimerTimestamp; i++) {
			in.readLong();
			in.readInt();
		}

		if (LOG.isDebugEnabled()) {
			int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

			if (restoredFromLegacyEventTimeTimers != null && !restoredFromLegacyEventTimeTimers.isEmpty()) {
				LOG.debug("{} (taskIdx={}) restored {} event time timers from an older Flink version: {}",
					getClass().getSimpleName(), subtaskIdx,
					restoredFromLegacyEventTimeTimers.size(),
					restoredFromLegacyEventTimeTimers);
			}

			if (restoredFromLegacyProcessingTimeTimers != null && !restoredFromLegacyProcessingTimeTimers.isEmpty()) {
				LOG.debug("{} (taskIdx={}) restored {} processing time timers from an older Flink version: {}",
					getClass().getSimpleName(), subtaskIdx,
					restoredFromLegacyProcessingTimeTimers.size(),
					restoredFromLegacyProcessingTimeTimers);
			}
		}
	}

	public void reregisterStateFromLegacyWindowOperator() {
		// if we restore from an older version,
		// we have to re-register the recovered state.

		if (restoredFromLegacyEventTimeTimers != null && !restoredFromLegacyEventTimeTimers.isEmpty()) {

			LOG.info("{} (taskIdx={}) re-registering event-time timers from an older Flink version.",
				getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask());

			for (Timer<K, W> timer : restoredFromLegacyEventTimeTimers) {
				setCurrentKey(timer.key);
				internalTimerService.registerEventTimeTimer(timer.window, timer.timestamp);
			}
		}

		if (restoredFromLegacyProcessingTimeTimers != null && !restoredFromLegacyProcessingTimeTimers.isEmpty()) {

			LOG.info("{} (taskIdx={}) re-registering processing-time timers from an older Flink version.",
				getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask());

			for (Timer<K, W> timer : restoredFromLegacyProcessingTimeTimers) {
				setCurrentKey(timer.key);
				internalTimerService.registerProcessingTimeTimer(timer.window, timer.timestamp);
			}
		}

		// gc friendliness
		restoredFromLegacyEventTimeTimers = null;
		restoredFromLegacyProcessingTimeTimers = null;
	}

	public void reregisterStateFromLegacyAlignedWindowOperator() throws Exception {
		if (restoredFromLegacyAlignedOpRecords != null && !restoredFromLegacyAlignedOpRecords.isEmpty()) {

			LOG.info("{} (taskIdx={}) re-registering timers from legacy {} from an older Flink version.",
				getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask(), legacyWindowOperatorType);

			while (!restoredFromLegacyAlignedOpRecords.isEmpty()) {
				StreamRecord<IN> record = restoredFromLegacyAlignedOpRecords.poll();
				setCurrentKey(keySelector.getKey(record.getValue()));
				processElement(record);
			}
		}

		// gc friendliness
		restoredFromLegacyAlignedOpRecords = null;
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Trigger<? super IN, ? super W> getTrigger() {
		return trigger;
	}

	@VisibleForTesting
	public KeySelector<IN, K> getKeySelector() {
		return keySelector;
	}

	@VisibleForTesting
	public WindowAssigner<? super IN, W> getWindowAssigner() {
		return windowAssigner;
	}

	@VisibleForTesting
	public StateDescriptor<? extends AppendingState<IN, ACC>, ?> getStateDescriptor() {
		return windowStateDescriptor;
	}
}

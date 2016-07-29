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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
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
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static java.util.Objects.requireNonNull;

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
	implements OneInputStreamOperator<IN, OUT>, Triggerable, InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	protected final WindowAssigner<? super IN, W> windowAssigner;

	protected final KeySelector<IN, K> keySelector;

	protected final Trigger<? super IN, ? super W> trigger;

	protected final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;

	/**
	 * This is used to copy the incoming element because it can be put into several window
	 * buffers.
	 */
	protected TypeSerializer<IN> inputSerializer;

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

	protected transient Map<Long, ScheduledFuture<?>> processingTimeTimerFutures;

	/**
	 * To keep track of the current watermark so that we can immediately fire if a trigger
	 * registers an event time callback for a timestamp that lies in the past.
	 */
	protected transient long currentWatermark = Long.MIN_VALUE;

	protected transient Context context = new Context(null, null);

	protected transient WindowAssigner.WindowAssignerContext windowAssignerContext;

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	/**
	 * Processing time timers that are currently in-flight.
	 */
	protected transient PriorityQueue<Timer<K, W>> processingTimeTimersQueue;
	protected transient Set<Timer<K, W>> processingTimeTimers;
	protected transient Multiset<Long> processingTimeTimerTimestamps;

	/**
	 * Current waiting watermark callbacks.
	 */
	protected transient Set<Timer<K, W>> watermarkTimers;
	protected transient PriorityQueue<Timer<K, W>> watermarkTimersQueue;

	protected transient Map<K, MergingWindowSet<W>> mergingWindowsByKey;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public WindowOperator(WindowAssigner<? super IN, W> windowAssigner,
		TypeSerializer<W> windowSerializer,
		KeySelector<IN, K> keySelector,
		TypeSerializer<K> keySerializer,
		StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
		InternalWindowFunction<ACC, OUT, K, W> windowFunction,
		Trigger<? super IN, ? super W> trigger,
		long allowedLateness) {

		super(windowFunction);

		this.windowAssigner = requireNonNull(windowAssigner);
		this.windowSerializer = windowSerializer;
		this.keySelector = requireNonNull(keySelector);
		this.keySerializer = requireNonNull(keySerializer);

		this.windowStateDescriptor = windowStateDescriptor;
		this.trigger = requireNonNull(trigger);

		Preconditions.checkArgument(allowedLateness >= 0);
		this.allowedLateness = allowedLateness;

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		currentWatermark = -1;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		inputSerializer = (TypeSerializer<IN>) type.createSerializer(executionConfig);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final void open() throws Exception {
		super.open();

		timestampedCollector = new TimestampedCollector<>(output);

		if (inputSerializer == null) {
			throw new IllegalStateException("Input serializer was not set.");
		}

		// these could already be initialized from restoreState()
		if (watermarkTimers == null) {
			watermarkTimers = new HashSet<>();
			watermarkTimersQueue = new PriorityQueue<>(100);
		}
		if (processingTimeTimers == null) {
			processingTimeTimers = new HashSet<>();
			processingTimeTimerTimestamps = HashMultiset.create();
			processingTimeTimersQueue = new PriorityQueue<>(100);
		}

		//ScheduledFutures are not checkpointed
		processingTimeTimerFutures = new HashMap<>();

		context = new Context(null, null);

		windowAssignerContext = new WindowAssigner.WindowAssignerContext() {
			@Override
			public long getCurrentProcessingTime() {
				return WindowOperator.this.getCurrentProcessingTime();
			}
		};

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindowsByKey = new HashMap<>();
		}

		currentWatermark = Long.MIN_VALUE;
	}

	@Override
	public final void close() throws Exception {
		super.close();
		timestampedCollector = null;
		watermarkTimers = null;
		watermarkTimersQueue = null;
		processingTimeTimers = null;
		processingTimeTimersQueue = null;
		context = null;
		windowAssignerContext = null;
		mergingWindowsByKey = null;
	}

	@Override
	public void dispose() {
		super.dispose();
		timestampedCollector = null;
		watermarkTimers = null;
		watermarkTimersQueue = null;
		processingTimeTimers = null;
		processingTimeTimersQueue = null;
		context = null;
		windowAssignerContext = null;
		mergingWindowsByKey = null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamRecord<IN> element) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		final K key = (K) getStateBackend().getCurrentKey();

		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window: elementWindows) {
				// If there is a merge, it can only result in a window that contains our new
				// element because we always eagerly merge
				final Tuple1<TriggerResult> mergeTriggerResult = new Tuple1<>(TriggerResult.CONTINUE);

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

						// store for later use
						mergeTriggerResult.f0 = context.onMerge(mergedWindows);

						for (W m: mergedWindows) {
							context.window = m;
							context.clear();
							deleteCleanupTimer(m);
						}

						// merge the merged state windows into the newly resulting state window
						getStateBackend().mergePartitionedStates(
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

				AppendingState<IN, ACC> windowState = getPartitionedState(
					stateWindow, windowSerializer, windowStateDescriptor);
				windowState.add(element.getValue());

				context.key = key;
				context.window = actualWindow;

				// we might have already fired because of a merge but still call onElement
				// on the (possibly merged) window
				TriggerResult triggerResult = context.onElement(element);
				TriggerResult combinedTriggerResult = TriggerResult.merge(triggerResult, mergeTriggerResult.f0);

				if (combinedTriggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					fire(actualWindow, contents);
				}

				if (combinedTriggerResult.isPurge()) {
					cleanup(actualWindow, windowState, mergingWindows);
				} else {
					registerCleanupTimer(actualWindow);
				}
			}
		} else {
			for (W window: elementWindows) {

				// drop if the window is already late
				if (isLate(window)) {
					continue;
				}

				AppendingState<IN, ACC> windowState = getPartitionedState(
					window, windowSerializer, windowStateDescriptor);
				windowState.add(element.getValue());

				context.key = key;
				context.window = window;

				TriggerResult triggerResult = context.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					fire(window, contents);
				}

				if (triggerResult.isPurge()) {
					cleanup(window, windowState, null);
				} else {
					registerCleanupTimer(window);
				}
			}
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		boolean fire;
		do {
			Timer<K, W> timer = watermarkTimersQueue.peek();
			if (timer != null && timer.timestamp <= mark.getTimestamp()) {
				fire = true;

				watermarkTimers.remove(timer);
				watermarkTimersQueue.remove();

				context.key = timer.key;
				context.window = timer.window;
				setKeyContext(timer.key);

				AppendingState<IN, ACC> windowState;
				MergingWindowSet<W> mergingWindows = null;

				if (windowAssigner instanceof MergingWindowAssigner) {
					mergingWindows = getMergingWindowSet();
					W stateWindow = mergingWindows.getStateWindow(context.window);
					if (stateWindow == null) {
						// then the window is already purged and this is a cleanup
						// timer set due to allowed lateness that has nothing to clean,
						// so it is safe to just ignore
						continue;
					}
					windowState = getPartitionedState(stateWindow, windowSerializer, windowStateDescriptor);
				} else {
					windowState = getPartitionedState(context.window, windowSerializer, windowStateDescriptor);
				}

				ACC contents = windowState.get();
				if (contents == null) {
					// if we have no state, there is nothing to do
					continue;
				}

				TriggerResult triggerResult = context.onEventTime(timer.timestamp);
				if (triggerResult.isFire()) {
					fire(context.window, contents);
				}

				if (triggerResult.isPurge() || (windowAssigner.isEventTime() && isCleanupTime(context.window, timer.timestamp))) {
					cleanup(context.window, windowState, mergingWindows);
				}

			} else {
				fire = false;
			}
		} while (fire);

		output.emitWatermark(mark);

		this.currentWatermark = mark.getTimestamp();
	}

	@Override
	public void trigger(long time) throws Exception {
		boolean fire;

		//Remove information about the triggering task
		processingTimeTimerFutures.remove(time);
		processingTimeTimerTimestamps.remove(time, processingTimeTimerTimestamps.count(time));

		do {
			Timer<K, W> timer = processingTimeTimersQueue.peek();
			if (timer != null && timer.timestamp <= time) {
				fire = true;

				processingTimeTimers.remove(timer);
				processingTimeTimersQueue.remove();

				context.key = timer.key;
				context.window = timer.window;
				setKeyContext(timer.key);

				AppendingState<IN, ACC> windowState;
				MergingWindowSet<W> mergingWindows = null;

				if (windowAssigner instanceof MergingWindowAssigner) {
					mergingWindows = getMergingWindowSet();
					W stateWindow = mergingWindows.getStateWindow(context.window);
					if (stateWindow == null) {
						// then the window is already purged and this is a cleanup
						// timer set due to allowed lateness that has nothing to clean,
						// so it is safe to just ignore
						continue;
					}
					windowState = getPartitionedState(stateWindow, windowSerializer, windowStateDescriptor);
				} else {
					windowState = getPartitionedState(context.window, windowSerializer, windowStateDescriptor);
				}

				ACC contents = windowState.get();
				if (contents == null) {
					// if we have no state, there is nothing to do
					continue;
				}

				TriggerResult triggerResult = context.onProcessingTime(timer.timestamp);
				if (triggerResult.isFire()) {
					fire(context.window, contents);
				}

				if (triggerResult.isPurge() || (!windowAssigner.isEventTime() && isCleanupTime(context.window, timer.timestamp))) {
					cleanup(context.window, windowState, mergingWindows);
				}

			} else {
				fire = false;
			}
		} while (fire);
	}

	/**
	 * Cleans up the window state if the provided {@link TriggerResult} requires so, or if it
	 * is time to do so (see {@link #isCleanupTime(Window, long)}). The caller must ensure that the
	 * correct key is set in the state backend and the context object.
	 */
	private void cleanup(W window,
						AppendingState<IN, ACC> windowState,
						MergingWindowSet<W> mergingWindows) throws Exception {
		windowState.clear();
		if (mergingWindows != null) {
			mergingWindows.retireWindow(window);
		}
		context.clear();
	}

	/**
	 * Triggers the window computation if the provided {@link TriggerResult} requires so.
	 * The caller must ensure that the correct key is set in the state backend and the context object.
	 */
	@SuppressWarnings("unchecked")
	private void fire(W window, ACC contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
		userFunction.apply(context.key, context.window, contents, timestampedCollector);
	}

	/**
	 * Retrieves the {@link MergingWindowSet} for the currently active key.
	 * The caller must ensure that the correct key is set in the state backend.
	 */
	@SuppressWarnings("unchecked")
	protected MergingWindowSet<W> getMergingWindowSet() throws Exception {
		MergingWindowSet<W> mergingWindows = mergingWindowsByKey.get((K) getStateBackend().getCurrentKey());
		if (mergingWindows == null) {
			// try to retrieve from state

			TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>((Class) Tuple2.class, new TypeSerializer[] {windowSerializer, windowSerializer} );
			ListStateDescriptor<Tuple2<W, W>> mergeStateDescriptor = new ListStateDescriptor<>("merging-window-set", tupleSerializer);
			ListState<Tuple2<W, W>> mergeState = getStateBackend().getPartitionedState(null, VoidSerializer.INSTANCE, mergeStateDescriptor);

			mergingWindows = new MergingWindowSet<>((MergingWindowAssigner<? super IN, W>) windowAssigner, mergeState);
			mergeState.clear();

			mergingWindowsByKey.put((K) getStateBackend().getCurrentKey(), mergingWindows);
		}
		return mergingWindows;
	}

	/**
	 * Decides if a window is currently late or not, based on the current
	 * watermark, i.e. the current event time, and the allowed lateness.
	 * @param window
	 * 					The collection of windows returned by the {@link WindowAssigner}.
	 * @return The windows (among the {@code eligibleWindows}) for which the element should still be
	 * 					considered when triggering.
	 */
	protected boolean isLate(W window) {
		return (windowAssigner.isEventTime() && (cleanupTime(window) <= currentWatermark));
	}

	/**
	 * Registers a timer to cleanup the content of the window.
	 * @param window
	 * 					the window whose state to discard
	 */
	protected void registerCleanupTimer(W window) {
		long cleanupTime = cleanupTime(window);
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
		long cleanupTime = window.maxTimestamp() + allowedLateness;
		return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
	}

	/**
	 * Decides if it is time to clean up the window state.
	 * Clean up time for a window is:
	 * 		<li> if it is event time, after the watermark passes the end of the window plus the user-specified allowed lateness
	 * 		<li> if it is processing time, after the processing time at the node passes the end of the window.
	 * 	@param window
	 * 					the window to clean
	 *  @param time
	 *  				the current time (event or processing depending on the {@link WindowAssigner}
	 *  @return {@code true} if it is time to clean up the window state, {@code false} otherwise.
	 */
	protected final boolean isCleanupTime(W window, long time) {
		long cleanupTime = cleanupTime(window);
		return  cleanupTime == time;
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
			return currentWatermark;
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			Class<S> stateType,
			S defaultState) {
			requireNonNull(stateType, "The state type class must not be null");

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

			requireNonNull(name, "The name of the state must not be null");
			requireNonNull(stateType, "The state type information must not be null");

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
					WindowOperator.this.getStateBackend().mergePartitionedStates(window,
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
			return WindowOperator.this.getCurrentProcessingTime();
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			// make sure we only put one timer per key into the queue
			if (processingTimeTimers.add(timer)) {
				processingTimeTimersQueue.add(timer);
				//If this is the first timer added for this timestamp register a TriggerTask
				if (processingTimeTimerTimestamps.add(time, 1) == 0) {
					ScheduledFuture<?> scheduledFuture = WindowOperator.this.registerTimer(time, WindowOperator.this);
					processingTimeTimerFutures.put(time, scheduledFuture);
				}
			}
		}

		@Override
		public void registerEventTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (watermarkTimers.add(timer)) {
				watermarkTimersQueue.add(timer);
			}
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);

			if (processingTimeTimers.remove(timer)) {
				processingTimeTimersQueue.remove(timer);
			}

			//If there are no timers left for this timestamp, remove it from queue and cancel TriggerTask
			if (processingTimeTimerTimestamps.remove(time,1) == 1) {
				ScheduledFuture<?> triggerTaskFuture = processingTimeTimerFutures.remove(timer.timestamp);
				if (triggerTaskFuture != null && !triggerTaskFuture.isDone()) {
					triggerTaskFuture.cancel(false);
				}
			}
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (watermarkTimers.remove(timer)) {
				watermarkTimersQueue.remove(timer);
			}
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

		public TriggerResult onMerge(Collection<W> mergedWindows) throws Exception {
			this.mergedWindows = mergedWindows;
			return trigger.onMerge(window, this);
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
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	@SuppressWarnings("unchecked")
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {

		if (mergingWindowsByKey != null) {
			TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>((Class) Tuple2.class, new TypeSerializer[] {windowSerializer, windowSerializer} );
			ListStateDescriptor<Tuple2<W, W>> mergeStateDescriptor = new ListStateDescriptor<>("merging-window-set", tupleSerializer);
			for (Map.Entry<K, MergingWindowSet<W>> key: mergingWindowsByKey.entrySet()) {
				setKeyContext(key.getKey());
				ListState<Tuple2<W, W>> mergeState = getStateBackend().getPartitionedState(null, VoidSerializer.INSTANCE, mergeStateDescriptor);
				mergeState.clear();
				key.getValue().persist(mergeState);
			}
		}

		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		AbstractStateBackend.CheckpointStateOutputView out =
			getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		snapshotTimers(out);

		taskState.setOperatorState(out.closeAndGetHandle());

		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState taskState) throws Exception {
		super.restoreState(taskState);

		final ClassLoader userClassloader = getUserCodeClassloader();

		@SuppressWarnings("unchecked")
		StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
		DataInputView in = inputState.getState(userClassloader);

		restoreTimers(in);
	}

	private void restoreTimers(DataInputView in ) throws IOException {
		int numWatermarkTimers = in.readInt();
		watermarkTimers = new HashSet<>(numWatermarkTimers);
		watermarkTimersQueue = new PriorityQueue<>(Math.max(numWatermarkTimers, 1));
		for (int i = 0; i < numWatermarkTimers; i++) {
			K key = keySerializer.deserialize(in);
			W window = windowSerializer.deserialize(in);
			long timestamp = in.readLong();
			Timer<K, W> timer = new Timer<>(timestamp, key, window);
			watermarkTimers.add(timer);
			watermarkTimersQueue.add(timer);
		}

		int numProcessingTimeTimers = in.readInt();
		processingTimeTimersQueue = new PriorityQueue<>(Math.max(numProcessingTimeTimers, 1));
		processingTimeTimers = new HashSet<>();
		for (int i = 0; i < numProcessingTimeTimers; i++) {
			K key = keySerializer.deserialize(in);
			W window = windowSerializer.deserialize(in);
			long timestamp = in.readLong();
			Timer<K, W> timer = new Timer<>(timestamp, key, window);
			processingTimeTimersQueue.add(timer);
			processingTimeTimers.add(timer);
		}

		int numProcessingTimeTimerTimestamp = in.readInt();
		processingTimeTimerTimestamps = HashMultiset.create();
		for (int i = 0; i< numProcessingTimeTimerTimestamp; i++) {
			long timestamp = in.readLong();
			int count = in.readInt();
			processingTimeTimerTimestamps.add(timestamp, count);
		}
	}

	private void snapshotTimers(DataOutputView out) throws IOException {
		out.writeInt(watermarkTimersQueue.size());
		for (Timer<K, W> timer : watermarkTimersQueue) {
			keySerializer.serialize(timer.key, out);
			windowSerializer.serialize(timer.window, out);
			out.writeLong(timer.timestamp);
		}

		out.writeInt(processingTimeTimers.size());
		for (Timer<K,W> timer : processingTimeTimers) {
			keySerializer.serialize(timer.key, out);
			windowSerializer.serialize(timer.window, out);
			out.writeLong(timer.timestamp);
		}

		out.writeInt(processingTimeTimerTimestamps.entrySet().size());
		for (Multiset.Entry<Long> timerTimestampCounts: processingTimeTimerTimestamps.entrySet()) {
			out.writeLong(timerTimestampCounts.getElement());
			out.writeInt(timerTimestampCounts.getCount());
		}
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

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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListStateDescriptor;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.guava18.com.google.common.base.Function;
import org.apache.flink.shaded.guava18.com.google.common.collect.FluentIterable;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link WindowOperator} that also allows an {@link Evictor} to be used.
 *
 * <p>The {@code Evictor} is used to remove elements from a pane before/after the evaluation of
 * {@link InternalWindowFunction} and after the window evaluation gets triggered by a
 * {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class EvictingWindowOperator<K, IN, OUT, W extends Window>
		extends WindowOperator<K, IN, Iterable<IN>, OUT, W> {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// these fields are set by the API stream graph builder to configure the operator

	private final Evictor<? super IN, ? super W> evictor;

	private final StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> evictingWindowStateDescriptor;

	// ------------------------------------------------------------------------
	// the fields below are instantiated once the operator runs in the runtime

	private transient EvictorContext evictorContext;

	private transient SubKeyedListState<Object, W, StreamRecord<IN>> evictingWindowState;

	// ------------------------------------------------------------------------

	public EvictingWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			KeySelector<IN, K> keySelector,
			TypeSerializer<K> keySerializer,
			StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> windowStateDescriptor,
			InternalWindowFunction<Iterable<IN>, OUT, K, W> windowFunction,
			Trigger<? super IN, ? super W> trigger,
			Evictor<? super IN, ? super W> evictor,
			long allowedLateness,
			OutputTag<IN> lateDataOutputTag) {

		super(windowAssigner, windowSerializer, keySelector,
			keySerializer, null, windowFunction, trigger, allowedLateness, lateDataOutputTag);

		this.evictor = checkNotNull(evictor);
		this.evictingWindowStateDescriptor = checkNotNull(windowStateDescriptor);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		final Collection<W> elementWindows = windowAssigner.assignWindows(
				element.getValue(), element.getTimestamp(), windowAssignerContext);

		//if element is handled by none of assigned elementWindows
		boolean isSkippedElement = true;

		final K key = this.<K>getKeyContext().getCurrentKey();

		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window : elementWindows) {

				// adding the new window might result in a merge, in that case the actualWindow
				// is the merged window and we work with that. If we don't merge then
				// actualWindow == window
				W actualWindow = mergingWindows.addWindow(window,
						new MergingWindowSet.MergeFunction<W>() {
							@Override
							public void merge(W mergeResult,
									Collection<W> mergedWindows, W stateWindowResult,
									Collection<W> mergedStateWindows) throws Exception {

								if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= internalTimerService.currentWatermark())) {
									throw new UnsupportedOperationException("The end timestamp of an " +
											"event-time window cannot become earlier than the current watermark " +
											"by merging. Current watermark: " + internalTimerService.currentWatermark() +
											" window: " + mergeResult);
								} else if (!windowAssigner.isEventTime() && mergeResult.maxTimestamp() <= internalTimerService.currentProcessingTime()) {
									throw new UnsupportedOperationException("The end timestamp of a " +
											"processing-time window cannot become earlier than the current processing time " +
											"by merging. Current processing time: " + internalTimerService.currentProcessingTime() +
											" window: " + mergeResult);
								}

								triggerContext.key = key;
								triggerContext.window = mergeResult;

								triggerContext.onMerge(mergedWindows);

								for (W m : mergedWindows) {
									triggerContext.window = m;
									triggerContext.clear();
									deleteCleanupTimer(m);
								}

								// merge the merged state windows into the newly resulting state window
								mergeNamespaces(evictingWindowState, stateWindowResult, mergedStateWindows);
							}
						});

				// drop if the window is already late
				if (isWindowLate(actualWindow)) {
					mergingWindows.retireWindow(actualWindow);
					continue;
				}
				isSkippedElement = false;

				W stateWindow = mergingWindows.getStateWindow(actualWindow);
				if (stateWindow == null) {
					throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
				}

				evictingWindowState.add(getCurrentKey(), stateWindow, element);

				triggerContext.key = key;
				triggerContext.window = actualWindow;
				evictorContext.key = key;
				evictorContext.window = actualWindow;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					Iterable<StreamRecord<IN>> contents = evictingWindowState.get(getCurrentKey(), stateWindow);
					if (contents == null) {
						// if we have no state, there is nothing to do
						continue;
					}
					emitWindowContents(actualWindow, contents, evictingWindowState, stateWindow);
				}

				if (triggerResult.isPurge()) {
					evictingWindowState.remove(getCurrentKey(), stateWindow);
				}
				registerCleanupTimer(actualWindow);
			}

			// need to make sure to update the merging state in state
			mergingWindows.persist();
		} else {
			for (W window : elementWindows) {

				// check if the window is already inactive
				if (isWindowLate(window)) {
					continue;
				}
				isSkippedElement = false;

				evictingWindowState.add(getCurrentKey(), window, element);

				triggerContext.key = key;
				triggerContext.window = window;
				evictorContext.key = key;
				evictorContext.window = window;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					Iterable<StreamRecord<IN>> contents = evictingWindowState.get(getCurrentKey(), window);
					if (contents == null) {
						// if we have no state, there is nothing to do
						continue;
					}
					emitWindowContents(window, contents, evictingWindowState, window);
				}

				if (triggerResult.isPurge()) {
					evictingWindowState.remove(getCurrentKey(), window);
				}
				registerCleanupTimer(window);
			}
		}

		// side output input event if
		// element not handled by any window
		// late arriving tag has been set
		// windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
		if (isSkippedElement && isElementLate(element)) {
			if (lateDataOutputTag != null){
				sideOutput(element);
			} else {
				this.numLateRecordsDropped.inc();
			}
		}
	}

	@Override
	public void onEventTime(InternalTimer<K, W> timer) throws Exception {

		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();
		evictorContext.key = timer.getKey();
		evictorContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows = null;

		W currNameSpace = null;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				return;
			} else {
				currNameSpace = stateWindow;
			}
		} else {
			currNameSpace = triggerContext.window;
		}

		Iterable<StreamRecord<IN>> contents = evictingWindowState.get(getCurrentKey(), currNameSpace);

		if (contents != null) {
			TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
			if (triggerResult.isFire()) {
				emitWindowContents(triggerContext.window, contents, evictingWindowState, currNameSpace);
			}
			if (triggerResult.isPurge()) {
				evictingWindowState.remove(getCurrentKey(), currNameSpace);
			}
		}

		if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, evictingWindowState, mergingWindows, currNameSpace);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();
		evictorContext.key = timer.getKey();
		evictorContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows = null;

		W currNamespace = null;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				return;
			} else {
				currNamespace = stateWindow;
			}
		} else {
			currNamespace = triggerContext.window;
		}

		Iterable<StreamRecord<IN>> contents = evictingWindowState.get(getCurrentKey(), currNamespace);

		if (contents != null) {
			TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());
			if (triggerResult.isFire()) {
				emitWindowContents(triggerContext.window, contents, evictingWindowState, currNamespace);
			}
			if (triggerResult.isPurge()) {
				evictingWindowState.remove(getCurrentKey(), currNamespace);
			}
		}

		if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, evictingWindowState, mergingWindows, currNamespace);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	private void emitWindowContents(
		W window,
		Iterable<StreamRecord<IN>> contents,
		SubKeyedListState<Object, W, StreamRecord<IN>> windowState,
		W currNamespace) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

		// Work around type system restrictions...
		FluentIterable<TimestampedValue<IN>> recordsWithTimestamp = FluentIterable
			.from(contents)
			.transform(new Function<StreamRecord<IN>, TimestampedValue<IN>>() {
				@Override
				public TimestampedValue<IN> apply(StreamRecord<IN> input) {
					return TimestampedValue.from(input);
				}
			});
		evictorContext.evictBefore(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

		FluentIterable<IN> projectedContents = recordsWithTimestamp
			.transform(new Function<TimestampedValue<IN>, IN>() {
				@Override
				public IN apply(TimestampedValue<IN> input) {
					return input.getValue();
				}
			});

		processContext.window = triggerContext.window;
		userFunction.process(triggerContext.key, triggerContext.window, processContext, projectedContents, timestampedCollector);
		evictorContext.evictAfter(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

		//work around to fix FLINK-4369, remove the evicted elements from the windowState.
		//this is inefficient, but there is no other way to remove elements from ListState, which is an AppendingState.
		windowState.remove(getCurrentKey(), currNamespace);
		for (TimestampedValue<IN> record : recordsWithTimestamp) {
			windowState.add(getCurrentKey(), currNamespace, record.getStreamRecord());
		}
	}

	private void clearAllState(
			W window,
			SubKeyedListState<Object, W, StreamRecord<IN>> windowState,
			MergingWindowSet<W> mergingWindows,
			W currNamespace) throws Exception {
		windowState.remove(getCurrentKey(), currNamespace);
		triggerContext.clear();
		processContext.window = window;
		processContext.clear();
		if (mergingWindows != null) {
			mergingWindows.retireWindow(window);
			mergingWindows.persist();
		}
	}

	/**
	 * {@code EvictorContext} is a utility for handling {@code Evictor} invocations. It can be reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
	 * the {@code EvictorContext}.
	 */

	class EvictorContext implements Evictor.EvictorContext {

		protected K key;
		protected W window;

		public EvictorContext(K key, W window) {
			this.key = key;
			this.window = window;
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
			return EvictingWindowOperator.this.getMetricGroup();
		}

		public K getKey() {
			return key;
		}

		void evictBefore(Iterable<TimestampedValue<IN>> elements, int size) {
			evictor.evictBefore((Iterable) elements, size, window, this);
		}

		void evictAfter(Iterable<TimestampedValue<IN>>  elements, int size) {
			evictor.evictAfter((Iterable) elements, size, window, this);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		evictorContext = new EvictorContext(null, null);
		SubKeyedListStateDescriptor<Object, W, StreamRecord<IN>> subKeyedListStateDescriptor =
			new SubKeyedListStateDescriptor<>(
				evictingWindowStateDescriptor.getName(),
				getKeySerializer(),
				windowSerializer,
				((ListStateDescriptor) evictingWindowStateDescriptor).getElementSerializer()
			);
		evictingWindowState = getSubKeyedState(subKeyedListStateDescriptor);
	}

	@Override
	public void close() throws Exception {
		super.close();
		evictorContext = null;
	}

	@Override
	public void dispose() throws Exception{
		super.dispose();
		evictorContext = null;
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Evictor<? super IN, ? super W> getEvictor() {
		return evictor;
	}

	@Override
	@VisibleForTesting
	@SuppressWarnings("unchecked, rawtypes")
	public StateDescriptor<? extends AppendingState<IN, Iterable<IN>>, ?> getStateDescriptor() {
		return (StateDescriptor<? extends AppendingState<IN, Iterable<IN>>, ?>) evictingWindowStateDescriptor;
	}

	private void mergeNamespaces(SubKeyedListState<Object, W, StreamRecord<IN>> state, W target, Collection<W> windows) {
		if (windows != null) {
			for (W window : windows) {
				List<StreamRecord<IN>> list = state.get(getCurrentKey(), window);
				state.addAll(getCurrentKey(), target, list);
				state.remove(getCurrentKey(), window);
			}
		}
	}
}

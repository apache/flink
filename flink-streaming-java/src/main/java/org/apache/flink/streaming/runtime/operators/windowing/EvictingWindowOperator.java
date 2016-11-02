/**
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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * A {@link WindowOperator} that also allows an {@link Evictor} to be used.
 *
 * <p>
 * The {@code Evictor} is used to remove elements from a pane before/after the evaluation of WindowFunction and
 * after the window evaluation gets triggered by a {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class EvictingWindowOperator<K, IN, OUT, W extends Window> extends WindowOperator<K, IN, Iterable<IN>, OUT, W> {

	private static final long serialVersionUID = 1L;

	private final Evictor<? super IN, ? super W> evictor;

	private transient EvictorContext evictorContext;

	private final StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> windowStateDescriptor;

	public EvictingWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
		TypeSerializer<W> windowSerializer,
		KeySelector<IN, K> keySelector,
		TypeSerializer<K> keySerializer,
		StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> windowStateDescriptor,
		InternalWindowFunction<Iterable<IN>, OUT, K, W> windowFunction,
		Trigger<? super IN, ? super W> trigger,
		Evictor<? super IN, ? super W> evictor,
		long allowedLateness) {

		super(windowAssigner, windowSerializer, keySelector,
			keySerializer, null, windowFunction, trigger, allowedLateness);
		this.evictor = requireNonNull(evictor);
		this.windowStateDescriptor = windowStateDescriptor;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamRecord<IN> element) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(
				element.getValue(),
				element.getTimestamp(),
				windowAssignerContext);

		final K key = (K) getKeyedStateBackend().getCurrentKey();

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
								context.key = key;
								context.window = mergeResult;

								context.onMerge(mergedWindows);

								for (W m : mergedWindows) {
									context.window = m;
									context.clear();
									deleteCleanupTimer(m);
								}

								// merge the merged state windows into the newly resulting state window
								getKeyedStateBackend().mergePartitionedStates(
									stateWindowResult,
									mergedStateWindows,
									windowSerializer,
									(StateDescriptor<? extends MergingState<?, ?>, ?>) windowStateDescriptor);
							}
						});

				// check if the window is already inactive
				if (isLate(actualWindow)) {
					mergingWindows.retireWindow(actualWindow);
					continue;
				}

				W stateWindow = mergingWindows.getStateWindow(actualWindow);
				if (stateWindow == null) {
					throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
				}
				ListState<StreamRecord<IN>> windowState = getPartitionedState(
					stateWindow, windowSerializer, windowStateDescriptor);
				windowState.add(element);

				context.key = key;
				context.window = actualWindow;
				evictorContext.key = key;
				evictorContext.window = actualWindow;

				TriggerResult triggerResult = context.onElement(element);

				if (triggerResult.isFire()) {
					Iterable<StreamRecord<IN>> contents = windowState.get();
					if (contents == null) {
						// if we have no state, there is nothing to do
						continue;
					}
					emitWindowContents(actualWindow, contents, windowState);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(actualWindow);
			}

			mergingWindows.persist();
		} else {
			for (W window : elementWindows) {

				// check if the window is already inactive
				if (isLate(window)) {
					continue;
				}

				ListState<StreamRecord<IN>> windowState = getPartitionedState(
					window, windowSerializer, windowStateDescriptor);
				windowState.add(element);

				context.key = key;
				context.window = window;
				evictorContext.key = key;
				evictorContext.window = window;

				TriggerResult triggerResult = context.onElement(element);

				if (triggerResult.isFire()) {
					Iterable<StreamRecord<IN>> contents = windowState.get();
					if (contents == null) {
						// if we have no state, there is nothing to do
						continue;
					}
					emitWindowContents(window, contents, windowState);
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
		evictorContext.key = timer.getKey();
		evictorContext.window = timer.getNamespace();

		ListState<StreamRecord<IN>> windowState;
		MergingWindowSet<W> mergingWindows = null;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(context.window);
			if (stateWindow == null) {
				// timer firing for non-existent window, still have to run the cleanup logic
				windowState = null;
			} else {
				windowState = getPartitionedState(
						stateWindow,
						windowSerializer,
						windowStateDescriptor);
			}
		} else {
			windowState = getPartitionedState(
					context.window,
					windowSerializer,
					windowStateDescriptor);
		}

		Iterable<StreamRecord<IN>> contents = null;
		if (windowState != null) {
			contents = windowState.get();
		}

		if (contents != null) {
			TriggerResult triggerResult = context.onEventTime(timer.getTimestamp());
			if (triggerResult.isFire()) {
				emitWindowContents(context.window, contents, windowState);
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
		evictorContext.key = timer.getKey();
		evictorContext.window = timer.getNamespace();

		ListState<StreamRecord<IN>> windowState;
		MergingWindowSet<W> mergingWindows = null;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(context.window);
			if (stateWindow == null) {
				// timer firing for non-existent window, still have to run the cleanup logic
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

		Iterable<StreamRecord<IN>> contents = null;
		if (windowState != null) {
			contents = windowState.get();
		}

		if (contents != null) {
			TriggerResult triggerResult = context.onProcessingTime(timer.getTimestamp());
			if (triggerResult.isFire()) {
				emitWindowContents(context.window, contents, windowState);
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

	private void emitWindowContents(W window, Iterable<StreamRecord<IN>> contents, ListState<StreamRecord<IN>> windowState) throws Exception {
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

		userFunction.apply(context.key, context.window, projectedContents, timestampedCollector);
		evictorContext.evictAfter(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));


		//work around to fix FLINK-4369, remove the evicted elements from the windowState.
		//this is inefficient, but there is no other way to remove elements from ListState, which is an AppendingState.
		windowState.clear();
		for(TimestampedValue<IN> record : recordsWithTimestamp) {
			windowState.add(record.getStreamRecord());
		}
	}

	private void clearAllState(
			W window,
			ListState<StreamRecord<IN>> windowState,
			MergingWindowSet<W> mergingWindows) throws Exception {

		windowState.clear();
		context.clear();
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
			evictor.evictBefore((Iterable)elements, size, window, this);
		}

		void evictAfter(Iterable<TimestampedValue<IN>>  elements, int size) {
			evictor.evictAfter((Iterable)elements, size, window, this);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();
		evictorContext = new EvictorContext(null,null);
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
		return (StateDescriptor<? extends AppendingState<IN, Iterable<IN>>, ?>) windowStateDescriptor;
	}
}

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
 * The {@code Evictor} is used to evict elements from panes before processing a window and after
 * a {@link Trigger} has fired.
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

				TriggerResult triggerResult = context.onElement(element);

				if (triggerResult.isFire()) {
					Iterable<StreamRecord<IN>> contents = windowState.get();
					if (contents == null) {
						// if we have no state, there is nothing to do
						continue;
					}
					emitWindowContents(actualWindow, contents);
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

				TriggerResult triggerResult = context.onElement(element);

				if (triggerResult.isFire()) {
					Iterable<StreamRecord<IN>> contents = windowState.get();
					if (contents == null) {
						// if we have no state, there is nothing to do
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

		ListState<StreamRecord<IN>> windowState;
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

		ListState<StreamRecord<IN>> windowState;
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

		Iterable<StreamRecord<IN>> contents = null;
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

	private void emitWindowContents(W window, Iterable<StreamRecord<IN>> contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

		// Work around type system restrictions...
		int toEvict = evictor.evict((Iterable) contents, Iterables.size(contents), context.window);

		FluentIterable<IN> projectedContents = FluentIterable
			.from(contents)
			.skip(toEvict)
			.transform(new Function<StreamRecord<IN>, IN>() {
				@Override
				public IN apply(StreamRecord<IN> input) {
					return input.getValue();
				}
			});
		userFunction.apply(context.key, context.window, projectedContents, timestampedCollector);
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

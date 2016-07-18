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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.watermark.Watermark;
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

		final K key = (K) getStateBackend().getCurrentKey();

		if (windowAssigner instanceof MergingWindowAssigner) {

			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window : elementWindows) {
				// If there is a merge, it can only result in a window that contains our new
				// element because we always eagerly merge
				final Tuple1<TriggerResult> mergeTriggerResult = new Tuple1<>(TriggerResult.CONTINUE);


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

								// store for later use
								mergeTriggerResult.f0 = context.onMerge(mergedWindows);

								for (W m : mergedWindows) {
									context.window = m;
									context.clear();
									deleteCleanupTimer(m);
								}

								// merge the merged state windows into the newly resulting state window
								getStateBackend().mergePartitionedStates(
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

				// we might have already fired because of a merge but still call onElement
				// on the (possibly merged) window
				TriggerResult triggerResult = context.onElement(element);
				TriggerResult combinedTriggerResult = TriggerResult.merge(triggerResult, mergeTriggerResult.f0);

				if (combinedTriggerResult.isFire()) {
					Iterable<StreamRecord<IN>> contents = windowState.get();
					if (contents == null) {
						// if we have no state, there is nothing to do
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

				ListState<StreamRecord<IN>> windowState;
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

				Iterable<StreamRecord<IN>> contents = windowState.get();
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

				ListState<StreamRecord<IN>> windowState;
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

				Iterable<StreamRecord<IN>> contents = windowState.get();
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

	private void fire(W window, Iterable<StreamRecord<IN>> contents) throws Exception {
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

	private void cleanup(W window,
						ListState<StreamRecord<IN>> windowState,
						MergingWindowSet<W> mergingWindows) throws Exception {

		windowState.clear();
		if (mergingWindows != null) {
			mergingWindows.retireWindow(window);
		}
		context.clear();
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

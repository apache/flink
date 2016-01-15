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
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
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
 * @param <OUT> The type of elements emitted by the {@code WindowFunction}.
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
		WindowFunction<Iterable<IN>, OUT, K, W> windowFunction,
		Trigger<? super IN, ? super W> trigger,
		Evictor<? super IN, ? super W> evictor) {
		super(windowAssigner, windowSerializer, keySelector, keySerializer, null, windowFunction, trigger);
		this.evictor = requireNonNull(evictor);
		this.windowStateDescriptor = windowStateDescriptor;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamRecord<IN> element) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(element.getValue(), element.getTimestamp());

		K key = (K) getStateBackend().getCurrentKey();

		for (W window: elementWindows) {

			ListState<StreamRecord<IN>> windowState = getPartitionedState(window, windowSerializer,
				windowStateDescriptor);

			windowState.add(element);

			context.key = key;
			context.window = window;
			TriggerResult triggerResult = context.onElement(element);

			processTriggerResult(triggerResult, key, window);
		}
	}

	@Override
	@SuppressWarnings("unchecked,rawtypes")
	protected void processTriggerResult(TriggerResult triggerResult, K key, W window) throws Exception {
		if (!triggerResult.isFire() && !triggerResult.isPurge()) {
			// do nothing
			return;
		}

		if (triggerResult.isFire()) {
			timestampedCollector.setTimestamp(window.maxTimestamp());

			setKeyContext(key);

			ListState<StreamRecord<IN>> windowState = getPartitionedState(window, windowSerializer,
				windowStateDescriptor);

			Iterable<StreamRecord<IN>> contents = windowState.get();

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

			if (triggerResult.isPurge()) {
				windowState.clear();
			} else {
				// we have to clear the state and set the elements that remain after eviction
				windowState.clear();
				for (StreamRecord<IN> rec: FluentIterable.from(contents).skip(toEvict)) {
					windowState.add(rec);
				}
			}
		} else if (triggerResult.isPurge()) {
			setKeyContext(key);
			ListState<StreamRecord<IN>> windowState = getPartitionedState(window, windowSerializer,
				windowStateDescriptor);
			windowState.clear();
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
	public StateDescriptor<? extends MergingState<IN, Iterable<IN>>, ?> getStateDescriptor() {
		return (StateDescriptor<? extends MergingState<IN, Iterable<IN>>, ?>) windowStateDescriptor;
	}
}

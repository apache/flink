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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.heap.HeapInternalStateBackend;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.api.operators.TestInternalTimerService;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockStreamConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility for testing {@link Trigger} behaviour.
 */
public class TriggerTestHarness<T, W extends Window> {

	private static final Integer KEY = 1;

	private final Trigger<T, W> trigger;
	private final TypeSerializer<W> windowSerializer;

	private final AbstractStreamOperator<Integer> operator;

	private final TestInternalTimerService<Integer, W> internalTimerService;

	public TriggerTestHarness(
			Trigger<T, W> trigger,
			TypeSerializer<W> windowSerializer) throws Exception {
		this.trigger = trigger;
		this.windowSerializer = windowSerializer;
		this.operator = new AbstractStreamOperator<Integer>() {
			private static final long serialVersionUID = 1L;
		};
		MockStreamConfig streamConfig = new MockStreamConfig();
		streamConfig.setStateKeySerializer(IntSerializer.INSTANCE);
		operator.setup(createMockStreamTask(), streamConfig, mock(Output.class));

		operator.initializeState();
		// we only ever use one key, other tests make sure that windows work across different
		// keys
		operator.setCurrentKey(KEY);

		this.internalTimerService = new TestInternalTimerService<>(new KeyContext() {
			@Override
			public void setCurrentKey(Object key) {
				// ignore
			}

			@Override
			public Object getCurrentKey() {
				return KEY;
			}
		});
	}

	public int numProcessingTimeTimers() {
		return internalTimerService.numProcessingTimeTimers();
	}

	public int numProcessingTimeTimers(W window) {
		return internalTimerService.numProcessingTimeTimers(window);
	}

	public int numEventTimeTimers() {
		return internalTimerService.numEventTimeTimers();
	}

	public int numEventTimeTimers(W window) {
		return internalTimerService.numEventTimeTimers(window);
	}

	public int numStateEntries() {
		return operator.getInternalStateBackend().numStateEntries();
	}

	public int numStateEntries(W window) {
		return ((HeapInternalStateBackend) operator.getInternalStateBackend()).numStateEntries(window);
	}

	/**
	 * Injects one element into the trigger for the given window and returns the result of
	 * {@link Trigger#onElement(Object, long, Window, Trigger.TriggerContext)}.
	 */
	public TriggerResult processElement(StreamRecord<T> element, W window) throws Exception {
		TestTriggerContext<Integer, W> triggerContext = new TestTriggerContext<>(
				KEY,
				window,
				internalTimerService,
				operator,
				windowSerializer);
		return trigger.onElement(element.getValue(), element.getTimestamp(), window, triggerContext);
	}

	/**
	 * Advanced processing time and checks whether we have exactly one firing for the given
	 * window. The result of {@link Trigger#onProcessingTime(long, Window, Trigger.TriggerContext)}
	 * is returned for that firing.
	 */
	public TriggerResult advanceProcessingTime(long time, W window) throws Exception {
		Collection<Tuple2<W, TriggerResult>> firings = advanceProcessingTime(time);

		if (firings.size() != 1) {
			throw new IllegalStateException("Must have exactly one timer firing. Fired timers: " + firings);
		}

		Tuple2<W, TriggerResult> firing = firings.iterator().next();

		if (!firing.f0.equals(window)) {
			throw new IllegalStateException("Trigger fired for another window.");
		}

		return firing.f1;
	}

	/**
	 * Advanced the watermark and checks whether we have exactly one firing for the given
	 * window. The result of {@link Trigger#onEventTime(long, Window, Trigger.TriggerContext)}
	 * is returned for that firing.
	 */
	public TriggerResult advanceWatermark(long time, W window) throws Exception {
		Collection<Tuple2<W, TriggerResult>> firings = advanceWatermark(time);

		if (firings.size() != 1) {
			throw new IllegalStateException("Must have exactly one timer firing. Fired timers: " + firings);
		}

		Tuple2<W, TriggerResult> firing = firings.iterator().next();

		if (!firing.f0.equals(window)) {
			throw new IllegalStateException("Trigger fired for another window.");
		}

		return firing.f1;
	}

	/**
	 * Advanced processing time and processes any timers that fire because of this. The
	 * window and {@link TriggerResult} for each firing are returned.
	 */
	public Collection<Tuple2<W, TriggerResult>> advanceProcessingTime(long time) throws Exception {
		Collection<TestInternalTimerService.Timer<Integer, W>> firedTimers =
				internalTimerService.advanceProcessingTime(time);

		Collection<Tuple2<W, TriggerResult>> result = new ArrayList<>();

		for (TestInternalTimerService.Timer<Integer, W> timer : firedTimers) {
			TestTriggerContext<Integer, W> triggerContext = new TestTriggerContext<>(
					KEY,
					timer.getNamespace(),
					internalTimerService,
					operator,
					windowSerializer);

			TriggerResult triggerResult =
					trigger.onProcessingTime(timer.getTimestamp(), timer.getNamespace(), triggerContext);

			result.add(new Tuple2<>(timer.getNamespace(), triggerResult));
		}

		return result;
	}

	/**
	 * Advanced the watermark and processes any timers that fire because of this. The
	 * window and {@link TriggerResult} for each firing are returned.
	 */
	public Collection<Tuple2<W, TriggerResult>> advanceWatermark(long time) throws Exception {
		Collection<TestInternalTimerService.Timer<Integer, W>> firedTimers =
				internalTimerService.advanceWatermark(time);

		Collection<Tuple2<W, TriggerResult>> result = new ArrayList<>();

		for (TestInternalTimerService.Timer<Integer, W> timer : firedTimers) {
			TriggerResult triggerResult = invokeOnEventTime(timer);
			result.add(new Tuple2<>(timer.getNamespace(), triggerResult));
		}

		return result;
	}

	private TriggerResult invokeOnEventTime(TestInternalTimerService.Timer<Integer, W> timer) throws Exception {
		TestTriggerContext<Integer, W> triggerContext = new TestTriggerContext<>(
				KEY,
				timer.getNamespace(),
				internalTimerService,
				operator,
				windowSerializer);

		return trigger.onEventTime(timer.getTimestamp(), timer.getNamespace(), triggerContext);
	}

	/**
	 * Manually invoke {@link Trigger#onEventTime(long, Window, Trigger.TriggerContext)} with
	 * the given parameters.
	 */
	public TriggerResult invokeOnEventTime(long timestamp, W window) throws Exception {
		TestInternalTimerService.Timer<Integer, W> timer =
				new TestInternalTimerService.Timer<>(timestamp, KEY, window);

		return invokeOnEventTime(timer);
	}

	/**
	 * Calls {@link Trigger#onMerge(Window, Trigger.OnMergeContext)} with the given
	 * parameters. This also calls {@link Trigger#clear(Window, Trigger.TriggerContext)} on the
	 * merged windows as does {@link WindowOperator}.
	 */
	public void mergeWindows(W targetWindow, Collection<W> mergedWindows) throws Exception {
		TestOnMergeContext<Integer, W> onMergeContext = new TestOnMergeContext<>(
				KEY,
				targetWindow,
				mergedWindows,
				internalTimerService,
				operator,
				windowSerializer);
		trigger.onMerge(targetWindow, onMergeContext);

		for (W mergedWindow : mergedWindows) {
			clearTriggerState(mergedWindow);
		}
	}

	/**
	 * Calls {@link Trigger#clear(Window, Trigger.TriggerContext)} for the given window.
	 */
	public void clearTriggerState(W window) throws Exception {
		TestTriggerContext<Integer, W> triggerContext = new TestTriggerContext<>(
				KEY,
				window,
				internalTimerService,
				operator,
				windowSerializer);
		trigger.clear(window, triggerContext);
	}

	private static class TestTriggerContext<K, W extends Window> implements Trigger.TriggerContext {

		protected final InternalTimerService<W> timerService;
		protected final K key;
		protected final W window;
		protected final TypeSerializer<W> windowSerializer;
		protected final AbstractStreamOperator<Integer> operator;

		TestTriggerContext(
				K key,
				W window,
				InternalTimerService<W> timerService,
				AbstractStreamOperator<Integer> operator,
				TypeSerializer<W> windowSerializer) throws Exception {
			this.key = key;
			this.window = window;
			this.timerService = timerService;
			this.operator = operator;
			this.windowSerializer = windowSerializer;
		}

		@Override
		public long getCurrentProcessingTime() {
			return timerService.currentProcessingTime();
		}

		@Override
		public MetricGroup getMetricGroup() {
			return null;
		}

		@Override
		public long getCurrentWatermark() {
			return timerService.currentWatermark();
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			timerService.registerProcessingTimeTimer(window, time);
		}

		@Override
		public void registerEventTimeTimer(long time) {
			timerService.registerEventTimeTimer(window, time);
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			timerService.deleteProcessingTimeTimer(window, time);
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			timerService.deleteEventTimeTimer(window, time);
		}

		@Override
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return operator.getContextStateHelper()
					.getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Error getting state", e);
			}
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(
				String name, Class<S> stateType, S defaultState) {
			return getPartitionedState(new ValueStateDescriptor<>(name, stateType, defaultState));
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(
				String name, TypeInformation<S> stateType, S defaultState) {
			return getPartitionedState(new ValueStateDescriptor<>(name, stateType, defaultState));
		}
	}

	private static class TestOnMergeContext<K, W extends Window> extends TestTriggerContext<K, W> implements Trigger.OnMergeContext {

		private final Collection<W> mergedWindows;

		public TestOnMergeContext(
				K key,
				W targetWindow,
				Collection<W> mergedWindows,
				InternalTimerService<W> timerService,
				AbstractStreamOperator<Integer> operator,
				TypeSerializer<W> windowSerializer) throws Exception {
			super(key, targetWindow, timerService, operator, windowSerializer);

			this.mergedWindows = mergedWindows;
		}

		@Override
		public <S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				S rawState = operator.getContextStateHelper().getOrCreateKeyedState(windowSerializer, stateDescriptor);
				if (rawState instanceof InternalMergingState) {
					@SuppressWarnings("unchecked")
					InternalMergingState<K, W, ?, ?, ?> mergingState = (InternalMergingState<K, W, ?, ?, ?>) rawState;
					mergingState.mergeNamespaces(window, mergedWindows);
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

	private static StreamTask<?, ?> createMockStreamTask() throws Exception {
		DummyEnvironment env = new DummyEnvironment("test", 1, 0);

		final CloseableRegistry closeableRegistry = new CloseableRegistry();
		StreamTask<?, ?> mockTask = mock(StreamTask.class);
		when(mockTask.getCheckpointLock()).thenReturn(new Object());
		when(mockTask.getConfiguration()).thenReturn(new MockStreamConfig());
		when(mockTask.getEnvironment()).thenReturn(env);
		when(mockTask.getExecutionConfig()).thenReturn(new ExecutionConfig());
		when(mockTask.getCancelables()).thenReturn(closeableRegistry);

		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		processingTimeService.setCurrentTime(0);

		StreamTaskStateInitializerImpl streamTaskStateInitializer = new StreamTaskStateInitializerImpl(
			env,
			new MemoryStateBackend(),
			processingTimeService);

		when(mockTask.createStreamTaskStateInitializer()).thenReturn(streamTaskStateInitializer);

		return mockTask;
	}
}

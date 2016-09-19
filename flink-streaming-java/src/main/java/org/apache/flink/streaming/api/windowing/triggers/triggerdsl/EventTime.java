/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.windowing.triggers.triggerdsl;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link DslTrigger} that reacts to event-time timers.
 * The behavior can be one of the following:
 * <p><ul>
 *      <li/> fire when the watermark passes the end of the window ({@link EventTime#afterEndOfWindow()}),
 *      <li/> fire when the event time advances by a certain interval
 *            after reception of the first element after the last firing for
 *            a given window ({@link EventTime#afterFirstElement(Time)}).
 * </ul></p>
 * In the first case, the trigger can also specify an <tt>early</tt> and a <tt>late</tt> trigger.
 * The <tt>early trigger</tt> will be responsible for specifying when the trigger should fire in the period
 * between the beginning of the window and the time when the watermark passes the end of the window.
 * The <tt>late trigger</tt> takes over after the watermark passes the end of the window, and specifies when
 * the trigger should fire in the period between the <tt>endOfWindow</tt> and <tt>endOfWindow + allowedLateness</tt>.
 *
 * @param <W> The type of {@link Window Windows} on which this {@code DslTrigger} can operate.
 */
public class EventTime<W extends Window> extends DslTrigger<Object, W> {
	private static final long serialVersionUID = 1L;

	private static final long UNDEFINED_INTERVAL = -1;

	private final long interval;

	private DslTrigger<Object, W> earlyTrigger;

	private DslTrigger<Object, W> lateTrigger;

	private DslTrigger<Object, W> actualTrigger;

	private EventTime(long interval) {
		Preconditions.checkArgument(interval >= 0 || interval == UNDEFINED_INTERVAL);
		this.interval = interval;
		if (this.interval == UNDEFINED_INTERVAL) {
			this.setIsRepeated(Repeated.UNDEFINED);
		} else {
			this.setIsRepeated(Repeated.ONCE);
		}
	}

	private boolean isAfterEndOfWindow() {
		return interval == UNDEFINED_INTERVAL;
	}

	/**
	 * In the case of an {@link #afterEndOfWindow()} trigger, this method allows the specification of an <tt>early</tt> trigger.
	 * This trigger will be responsible for specifying when the trigger should fire in the period between the beginning
	 * of the window and the time when the watermark passes the end of the window. If no early trigger is specified, then
	 * no firing happens within this period.
	 *
	 * @param earlyFiringTrigger The specification of the early trigger.
	 */
	public EventTime<W> withEarlyTrigger(DslTrigger<Object, W> earlyFiringTrigger) {
		this.earlyTrigger = earlyFiringTrigger;
		return this;
	}

	/**
	 * In the case of an {@link #afterEndOfWindow()} trigger, this method allows the specification of an <tt>late</tt> trigger.
	 * This trigger takes over after the watermark passes the end of the window, and specifies when the trigger should fire
	 * in the period between the <tt>endOfWindow</tt> and <tt>endOfWindow + allowedLateness</tt>. If no late trigger is specified,
	 * then we have a firing for every late element that arrives.
	 *
	 * @param lateFiringTrigger The specification of the late trigger.
	 * */
	public EventTime<W> withLateTrigger(DslTrigger<Object, W> lateFiringTrigger) {
		this.lateTrigger = lateFiringTrigger;
		return this;
	}

	@Override
	List<DslTrigger<Object, W>> getChildTriggers() {
		return this.actualTrigger.getChildTriggers();
	}

	@Override
	DslTrigger<Object, W> translate(TypeSerializer<W> windowSerializer, long allowedLateness) {
		if (isAfterEndOfWindow() && windowSerializer instanceof GlobalWindow.Serializer) {
			throw new IllegalStateException("An EventTimeTrigger.afterEndOfWindow() will never fire with GlobalWindows.");
		}

		if (earlyTrigger != null && !isAfterEndOfWindow()) {
			throw new UnsupportedOperationException("An EventTimeTrigger.afterFirstElement() cannot have early firings.");
		}

		if (lateTrigger != null && !isAfterEndOfWindow()) {
			throw new UnsupportedOperationException("An EventTimeTrigger.afterFirstElement() cannot have late firings.");
		}

		if (isAfterEndOfWindow()) {
			this.actualTrigger = lateTrigger == null || allowedLateness == 0 ?
				new AfterEndOfWindowNoState<>(earlyTrigger) :
				new AfterEndOfWindow<>(earlyTrigger, lateTrigger);
		} else {
			this.actualTrigger = new AfterFirstElementInPane<>(interval);
		}
		this.actualTrigger = actualTrigger.translate(windowSerializer, allowedLateness);
		return this;
	}

	@Override
	<S extends State> List<StateDescriptor<S, ?>> getStateDescriptors() {
		return this.actualTrigger.getStateDescriptors();
	}

	@Override
	boolean onElement(Object element, long timestamp, W window, DslTriggerContext context) throws Exception {
		return this.actualTrigger.onElement(element, timestamp, window, context);
	}

	@Override
	boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, W window, DslTriggerContext context) throws Exception {
		return this.actualTrigger.shouldFireOnTimer(time, isEventTimeTimer, window, context);
	}

	@Override
	void onFire(W window, DslTriggerContext context) throws Exception {
		this.actualTrigger.onFire(window, context);
	}

	@Override
	void clear(W window, DslTriggerContext context) throws Exception {
		this.actualTrigger.clear(window, context);
	}

	@Override
	public boolean canMerge() {
		return true;
	}

	@Override
	boolean onMerge(W window, DslTriggerContext context) throws Exception {
		return this.actualTrigger.onMerge(window, context);
	}

	@Override
	public String toString() {
		return !isAfterEndOfWindow() ?
			"EventTimeTrigger.afterFirstElement(" + interval + " ms)" :
			"EventTimeTrigger.afterEndOfWindow()" +
				(earlyTrigger == null ? "" : ".withEarlyTrigger(" + earlyTrigger.toString() + ")") +
				(lateTrigger == null ? "" : ".withLateTrigger(" + lateTrigger.toString() + ")");
	}

	/**
	 * Creates a trigger that fires when the watermark passes the end of the window.
	 * This trigger allows the additional specification of an early (see {@link #withEarlyTrigger(DslTrigger)})
	 * and/or a late trigger (see {@link #withLateTrigger(DslTrigger)}).
	 */
	public static <W extends Window> EventTime<W> afterEndOfWindow() {
		return new EventTime<>(UNDEFINED_INTERVAL);
	}

	/**
	 * Creates a trigger that fires when the event time advances by a certain <tt>interval</tt> after reception of
	 * the first element after the last firing of a given window.
	 * @param interval The interval by which the event time should advance.
	 */
	public static <W extends Window> EventTime<W> afterFirstElement(Time interval) {
		return new EventTime<>(interval.toMilliseconds());
	}

	public static <W extends Window> EventTime<W> Default() {
		return EventTime.afterEndOfWindow();
	}

	/**
	 * Upon merging it returns the minimum among the values that are different than -1.
	 */
	private static class MinAmongSet implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return (value1 == -1L || value2 == -1L) ?
				value1 + value2 + 1L :
				Math.min(value1, value2);
		}
	}

	private class AfterFirstElementInPane<P extends Window> extends DslTrigger<Object, P> {

		private final long interval;

		private ReducingStateDescriptor<Long> afterFirstElementStateDesc;

		AfterFirstElementInPane(long interval) {
			Preconditions.checkArgument(interval >= 0 || interval == UNDEFINED_INTERVAL);
			this.interval = interval;
		}

		@Override
		List<DslTrigger<Object, P>> getChildTriggers() {
			return new ArrayList<>();
		}

		@Override
		DslTrigger<Object, P> translate(TypeSerializer<P> windowSerializer, long allowedLateness) {
			String descriptorName = "eventTime-afterFirstElement_" + interval;

			this.afterFirstElementStateDesc = new ReducingStateDescriptor<>(
				descriptorName, new MinAmongSet(), LongSerializer.INSTANCE);
			return this;
		}

		@Override
		List<ReducingStateDescriptor<Long>> getStateDescriptors() {
			if (afterFirstElementStateDesc == null) {
				throw new IllegalStateException("The state descriptor has not been initialized. " +
					"Please call translate() before asking for state descriptors.");
			}
			List<ReducingStateDescriptor<Long>> descriptors = new ArrayList<>();
			descriptors.add(this.afterFirstElementStateDesc);
			return descriptors;
		}

		@Override
		boolean onElement(Object element, long timestamp, P window, DslTriggerContext context) throws Exception {
			// this is the afterFirstElementInPane mode
			ReducingState<Long> nextFiring = context.getPartitionedState(afterFirstElementStateDesc);
			Long timer = nextFiring.get();

			if (timer == null) {
				long nextTimer = context.getCurrentWatermark() + interval;
				context.registerEventTimeTimer(nextTimer);
				nextFiring.add(nextTimer);
			} else {
				// this is the case after a merge
				context.registerEventTimeTimer(timer);
			}
			return false;
		}

		@Override
		boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, P window, DslTriggerContext context) throws Exception {
			if (!isEventTimeTimer) {
				return false;
			}
			ReducingState<Long> nextFiring = context.getPartitionedState(afterFirstElementStateDesc);
			Long timer = nextFiring.get();
			return timer != null && timer == time;
		}

		@Override
		void onFire(P window, DslTriggerContext context) throws Exception {
			context.getPartitionedState(afterFirstElementStateDesc).clear();
		}

		@Override
		public boolean canMerge() {
			return true;
		}

		@Override
		boolean onMerge(P window, DslTriggerContext context) throws Exception {
			// the onElement will take care of setting a new timer
			// if we are in afterEndOfWindow mode, or the merging
			// revealed no already set timers

			context.mergePartitionedState(afterFirstElementStateDesc);
			return false;
		}

		@Override
		public String toString() {
			return "EventTimeTrigger.afterFirstElement(" + interval + " ms)";
		}

		@Override
		void clear(P window, DslTriggerContext context) throws Exception {
			ReducingState<Long> nextFiring = context.getPartitionedState(afterFirstElementStateDesc);
			nextFiring.clear();
		}
	}

	private class AfterEndOfWindow<P extends Window> extends DslTrigger<Object, P> {

		private final DslTrigger<Object, P> earlyTrigger;

		private final DslTrigger<Object, P> lateTrigger;

		private ValueStateDescriptor<Boolean> hasFiredOnTimeStateDesc;

		AfterEndOfWindow(DslTrigger<Object, P> earlyTrigger, DslTrigger<Object, P> lateTrigger) {
			this.earlyTrigger = earlyTrigger;
			this.lateTrigger = lateTrigger;
		}

		@Override
		public List<DslTrigger<Object, P>> getChildTriggers() {
			List<DslTrigger<Object, P>> triggers = new ArrayList<>();
			if (earlyTrigger != null) {
				triggers.add(earlyTrigger);
			}

			if (lateTrigger != null) {
				triggers.add(lateTrigger);
			}
			return triggers;
		}

		private DslTriggerInvokable<Object, P> getEarlyTriggerInvokable(DslTriggerContext ctx) {
			if (earlyTrigger == null) {
				return null;
			}
			return ctx.getChildInvokable(0);
		}

		private DslTriggerInvokable<Object, P> getLateTriggerInvokable(DslTriggerContext ctx) {
			if (lateTrigger == null) {
				return null;
			} else if (earlyTrigger == null) {
				return ctx.getChildInvokable(0);
			}
			return ctx.getChildInvokable(1);
		}


		@Override
		DslTrigger<Object, P> translate(TypeSerializer<P> windowSerializer, long allowedLateness) {
			String descriptorName = "eventTime-afterEOW";
			this.hasFiredOnTimeStateDesc = new ValueStateDescriptor<>(
				descriptorName, BooleanSerializer.INSTANCE, false);
			return this;
		}

		@Override
		List<ValueStateDescriptor<Boolean>> getStateDescriptors() {
			if (this.hasFiredOnTimeStateDesc == null) {
				throw new IllegalStateException("The state descriptor has not been initialized. " +
					"Please call translate() before asking for state descriptors.");
			}
			List<ValueStateDescriptor<Boolean>> descriptors = new ArrayList<>();
			descriptors.add(this.hasFiredOnTimeStateDesc);
			return descriptors;
		}

		@Override
		boolean onElement(Object element, long timestamp, P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			boolean earlyTriggerResult = earlyTriggerInvokable != null &&
				earlyTriggerInvokable.invokeOnElement(element, timestamp, window);

			DslTriggerInvokable<Object, P> lateTriggerInvokable = getLateTriggerInvokable(context);
			boolean lateTriggerResult = lateTriggerInvokable != null &&
				lateTriggerInvokable.invokeOnElement(element, timestamp, window);

			Boolean hasFiredOnTime = context.getPartitionedState(hasFiredOnTimeStateDesc).value();
			if (hasFiredOnTime) {
				// this is to cover the case where we recover from a failure and the watermark
				// is Long.MIN_VALUE but the window is already in the late phase.
				return lateTriggerInvokable != null && lateTriggerResult;
			} else {
				if (window.maxTimestamp() <= context.getCurrentWatermark()) {
					// we are in the late phase

					// if there is no late trigger then we fire on every late element
					// This also covers the case of recovery after a failure
					// where the currentWatermark will be Long.MIN_VALUE
					return true;
				} else {
					// we are in the early phase
					context.registerEventTimeTimer(window.maxTimestamp());
					return earlyTriggerResult;
				}
			}
		}

		@Override
		boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			boolean shouldEarlyFire = earlyTriggerInvokable != null && earlyTriggerInvokable.invokeShouldFire(time, isEventTimeTimer, window);

			DslTriggerInvokable<Object, P> lateTriggerInvokable = getLateTriggerInvokable(context);
			boolean shouldLateFire = lateTriggerInvokable != null && lateTriggerInvokable.invokeShouldFire(time, isEventTimeTimer, window);

			Boolean hasFiredOnTime = context.getPartitionedState(hasFiredOnTimeStateDesc).value();
			if (hasFiredOnTime) {
				return shouldLateFire;
			} else {
				return (isEventTimeTimer && time == window.maxTimestamp()) || shouldEarlyFire;
			}
		}

		@Override
		void onFire(P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			if (earlyTriggerInvokable != null) {
				earlyTriggerInvokable.invokeOnFire(window);
			}

			DslTriggerInvokable<Object, P> lateTriggerInvokable = getLateTriggerInvokable(context);
			if (lateTriggerInvokable != null) {
				lateTriggerInvokable.invokeOnFire(window);
			}

			if (context.getCurrentWatermark() >= window.maxTimestamp()) {
				ValueState<Boolean> hasFiredState = context.getPartitionedState(hasFiredOnTimeStateDesc);
				if (!hasFiredState.value()) {
					if (lateTriggerInvokable != null) {
						lateTriggerInvokable.invokeClear(window);
					}
					if (earlyTriggerInvokable != null) {
						earlyTriggerInvokable.invokeClear(window);
					}
					hasFiredState.update(true);
				}
			}
		}

		@Override
		public boolean canMerge() {
			return (earlyTrigger == null || earlyTrigger.canMerge()) &&
				(lateTrigger == null || lateTrigger.canMerge());
		}

		@Override
		boolean onMerge(P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			boolean earlyTriggerResult = earlyTriggerInvokable != null &&
				earlyTriggerInvokable.invokeOnMerge(window);

			DslTriggerInvokable<Object, P> lateTriggerInvokable = getLateTriggerInvokable(context);
			boolean lateTriggerResult = lateTriggerInvokable != null &&
				lateTriggerInvokable.invokeOnMerge(window);

			// we assume that the new merged window has not fired yet its on-time timer.
			context.getPartitionedState(hasFiredOnTimeStateDesc).update(false);

			// the onElement() will register the timer for the end of the new window.
			return window.maxTimestamp() <= context.getCurrentWatermark() ?
				lateTriggerResult : earlyTriggerResult;
		}

		@Override
		public String toString() {
			return "EventTimeTrigger.afterEndOfWindow()" +
				(earlyTrigger == null ? "" : ".withEarlyTrigger("+ earlyTrigger.toString() +")") +
				(lateTrigger == null ? "" : ".withLateTrigger("+ lateTrigger.toString() +")");
		}

		@Override
		void clear(P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			if (earlyTriggerInvokable != null) {
				earlyTriggerInvokable.invokeClear(window);
			}

			DslTriggerInvokable<Object, P> lateTriggerInvokable = getLateTriggerInvokable(context);
			if (lateTriggerInvokable != null) {
				lateTriggerInvokable.invokeClear(window);
			}
			context.getPartitionedState(hasFiredOnTimeStateDesc).clear();
		}
	}

	private class AfterEndOfWindowNoState<P extends Window> extends DslTrigger<Object, P> {

		private final DslTrigger<Object, P> earlyTrigger;

		AfterEndOfWindowNoState(DslTrigger<Object, P> earlyTrigger) {
			this.earlyTrigger = earlyTrigger;
		}

		@Override
		List<DslTrigger<Object, P>> getChildTriggers() {
			List<DslTrigger<Object, P>> triggers = new ArrayList<>();
			if (earlyTrigger != null) {
				triggers.add(earlyTrigger);
			}
			return triggers;
		}

		DslTriggerInvokable<Object, P> getEarlyTriggerInvokable(DslTriggerContext ctx) {
			if (earlyTrigger == null) {
				return null;
			}
			return ctx.getChildInvokable(0);
		}

		@Override
		DslTrigger<Object, P> translate(TypeSerializer<P> windowSerializer, long allowedLateness) {
			return this;
		}

		@Override
		<S extends State> List<StateDescriptor<S, ?>> getStateDescriptors() {
			return Collections.emptyList();
		}

		@Override
		boolean onElement(Object element, long timestamp, P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			boolean earlyTriggerResult = earlyTriggerInvokable != null &&
				earlyTriggerInvokable.invokeOnElement(element, timestamp, window);
			if (window.maxTimestamp() <= context.getCurrentWatermark()) {
				// the on-time firing
				return true;
			} else {
				// this is an early element so register the timer and let the early trigger decide
				context.registerEventTimeTimer(window.maxTimestamp());
				return earlyTriggerResult;
			}
		}

		@Override
		boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			boolean shouldEarlyFire = earlyTriggerInvokable != null && earlyTriggerInvokable.invokeShouldFire(time, isEventTimeTimer, window);
			return (isEventTimeTimer && time == window.maxTimestamp()) || shouldEarlyFire;
		}

		@Override
		void onFire(P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			if (earlyTriggerInvokable != null) {
				earlyTriggerInvokable.invokeOnFire(window);
			}
		}

		@Override
		public boolean canMerge() {
			return (earlyTrigger == null || earlyTrigger.canMerge());
		}

		@Override
		boolean onMerge(P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			boolean earlyTriggerResult = earlyTriggerInvokable != null &&
				earlyTriggerInvokable.invokeOnMerge(window);

			// the onElement() will register the timer for the end of the new window.
			return window.maxTimestamp() > context.getCurrentWatermark() && earlyTriggerResult;
		}

		@Override
		public String toString() {
			return "EventTimeTrigger.afterEndOfWindow()" +
				(earlyTrigger == null ? "" : ".withEarlyTrigger(" + earlyTrigger.toString() + ")");
		}

		@Override
		void clear(P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			if (earlyTriggerInvokable != null) {
				earlyTriggerInvokable.invokeClear(window);
			}
		}
	}
}

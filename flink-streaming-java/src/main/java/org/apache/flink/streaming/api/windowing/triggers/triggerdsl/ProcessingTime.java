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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link DslTrigger} that reacts to processing-time timers.
 * The behavior can be one of the following:
 * <p><ul>
 *     <li> fire when the processing time passes the end of the window ({@link ProcessingTime#afterEndOfWindow()}),
 *     <li> fire when the processing time advances by a certain interval
 *          after reception of the first element after the last firing for
 *          a given window ({@link ProcessingTime#afterFirstElement(Time)}).
 * </ul></p>
 * In the first case, the trigger can also specify an <tt>early</tt> trigger.
 * The <tt>early trigger</tt> will be responsible for specifying when the trigger should fire in the period
 * between the beginning of the window and the time when the processing time passes the end of the window.
 *
 * @param <W> The type of {@link Window Windows} on which this {@code DslTrigger} can operate.
 */
public class ProcessingTime<W extends Window> extends DslTrigger<Object, W> {
	private static final long serialVersionUID = 1L;

	private static final long UNDEFINED_INTERVAL = -1;

	private final long interval;

	private DslTrigger<Object, W> earlyTrigger;

	private DslTrigger<Object, W> actualTrigger;

	private ProcessingTime(long interval) {
		// we do not check it here but if the interval is bigger than the
		// duration of the window + allowed lateness, then the trigger will not fire.
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
	 * of the window and the time when the processing time passes the end of the window. If no early trigger is specified, then
	 * no firing happens within this period.
	 *
	 * @param earlyFiringTrigger The specification of the early trigger.
	 */
	public ProcessingTime<W> withEarlyTrigger(DslTrigger<Object, W> earlyFiringTrigger) {
		this.earlyTrigger = earlyFiringTrigger;
		return this;
	}

	@Override
	List<DslTrigger<Object, W>> getChildTriggers() {
		List<DslTrigger<Object, W>> triggers = new ArrayList<>();
		if (earlyTrigger != null) {
			triggers.add(earlyTrigger);
		}
		return triggers;
	}

	@Override
	DslTrigger<Object, W> translate(TypeSerializer<W> windowSerializer, long allowedLateness) {
		if (isAfterEndOfWindow() && windowSerializer instanceof GlobalWindow.Serializer) {
			throw new IllegalStateException("A ProcessingTimeTrigger.afterEndOfWindow() will never fire with GlobalWindows.");
		} else if (!isAfterEndOfWindow() && earlyTrigger != null) {
			throw new UnsupportedOperationException("ProcessingTimeTrigger.afterFirstElement() cannot have early firings.");
		}

		this.actualTrigger = isAfterEndOfWindow() ?
			new AfterEndOfWindow<>(earlyTrigger) :
			new AfterFirstElementInPane<W>(interval);

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
		return this.actualTrigger.canMerge();
	}

	@Override
	boolean onMerge(W window, DslTriggerContext context) throws Exception {
		return this.actualTrigger.onMerge(window, context);
	}

	@Override
	public String toString() {
		return !isAfterEndOfWindow() ?
			"ProcessingTimeTrigger.afterFirstElement(" + interval + " ms)" :
			"ProcessingTimeTrigger.afterEndOfWindow()" +
				(earlyTrigger == null ? "" : ".withEarlyTrigger(" + earlyTrigger.toString() + ")");
	}

	/**
	 * Creates a trigger that fires when the processing time passes the end of the window.
	 * This trigger allows the additional specification of an early (see {@link #withEarlyTrigger(DslTrigger)}).
	 */
	public static <W extends Window> ProcessingTime<W> afterEndOfWindow() {
		return new ProcessingTime<>(UNDEFINED_INTERVAL);
	}

	/**
	 * Creates a trigger that fires when the processing time advances by a certain <tt>interval</tt>
	 * after reception of the first element after the last firing of a given window.
	 * @param interval The interval by which the event time should advance.
	 */
	public static <W extends Window> ProcessingTime<W> afterFirstElement(Time interval) {
		return new ProcessingTime<>(interval.toMilliseconds());
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
			String descriptorName = "processingTime-afterFirstElement_" + interval;
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
			ReducingState<Long> nextFiring = context.getPartitionedState(afterFirstElementStateDesc);
			Long timer = nextFiring.get();

			if (timer == null) {
				long nextTimer = context.getCurrentProcessingTime() + interval;
				context.registerProcessingTimeTimer(nextTimer);
				nextFiring.add(nextTimer);
			} else {
				// this is the case after a merge
				context.registerProcessingTimeTimer(timer);
			}
			return false;
		}

		@Override
		boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, P window, DslTriggerContext context) throws Exception {
			if (isEventTimeTimer) {
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

			// We are in processing time. In the context of session windows,
			// a new element can only expand the upper bound of a window.
			// It cannot merge two already existing windows.
			// So this trigger, the only thing it has to do is just
			// reset the previous processing time timer, if there is any, or
			// set a new timer because this is the first element.

			context.mergePartitionedState(afterFirstElementStateDesc);
			return false;
		}

		@Override
		public String toString() {
			return "ProcessingTimeTrigger.afterFirstElement(" + interval + " ms)";
		}

		@Override
		void clear(P window, DslTriggerContext context) throws Exception {
			ReducingState<Long> nextFiring = context.getPartitionedState(afterFirstElementStateDesc);
			nextFiring.clear();
		}
	}

	private class AfterEndOfWindow<P extends Window> extends DslTrigger<Object, P> {

		private final DslTrigger<Object, P> earlyTrigger;

		AfterEndOfWindow(DslTrigger<Object, P> earlyTrigger) {
			this.earlyTrigger = earlyTrigger;
		}

		@Override
		List<DslTrigger<Object, P>> getChildTriggers() {
			return new ArrayList<>();
		}

		private DslTriggerInvokable<Object, P> getEarlyTriggerInvokable(DslTriggerContext ctx) {
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
			context.registerProcessingTimeTimer(window.maxTimestamp());
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			return earlyTriggerInvokable != null &&
				earlyTriggerInvokable.invokeOnElement(element, timestamp, window);
		}

		@Override
		boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			boolean shouldEarlyFire = earlyTriggerInvokable != null &&
				earlyTriggerInvokable.invokeShouldFire(time, isEventTimeTimer, window);
			return (!isEventTimeTimer && time == window.maxTimestamp()) || shouldEarlyFire;
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
			return earlyTrigger == null || earlyTrigger.canMerge();
		}

		@Override
		boolean onMerge(P window, DslTriggerContext context) throws Exception {
			DslTriggerInvokable<Object, P> earlyTriggerInvokable = getEarlyTriggerInvokable(context);
			return earlyTriggerInvokable != null &&
				earlyTriggerInvokable.invokeOnMerge(window);
		}

		@Override
		public String toString() {
			return "ProcessingTimeTrigger.afterEndOfWindow()" +
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

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

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.Serializable;
import java.util.List;

/**
 * The specification of a dsl trigger.
 *
 * @param <T> The type of elements on which this {@code Trigger} works.
 * @param <W> The type of {@link Window Windows} on which this {@code Trigger} can operate.
 */
public abstract class DslTrigger<T, W extends Window> implements Serializable {

	private static final long serialVersionUID = -4104633972991191369L;

	/**
	 * Flag indicating if the trigger purges the window state after every firing
	 * (<tt>discarding</tt>), or it only purges it at clean up time (<tt>accumulating</tt>).
	 * By default triggers operate in <tt>accumulating</tt> mode.
	 */
	private boolean isDiscarding = false;

	/**
	 * Flag indicating if the trigger should fire throughout the lifespan of
	 * a window, i.e. <tt>[start ... end + allowedLateness]</tt>, or fire once and
	 * then never again.
	 */
	private Repeated isRepeated = Repeated.UNDEFINED;

	/**
	 * Makes the trigger operate in <tt>discarding</tt> mode, i.e.
	 * purge the window state after every firing. By default dsl triggers
	 * operate in <tt>accumulating</tt> mode, i.e. they do not purge the
	 * window state after each firing, but only at clean up time.
	 */
	public DslTrigger<T, W> discarding() {
		setIsDiscarding(true);
		return this;
	}

	/**
	 * Makes the trigger operate in <tt>accumulating</tt> mode, i.e.
	 * it does not purge the window state after every firing, but only
	 * at clean up time. This is the default mode.
	 */
	public DslTrigger<T, W> accumulating() {
		setIsDiscarding(false);
		return this;
	}

	boolean isDiscarding() {
		return this.isDiscarding;
	}

	Repeated isRepeated() {
		return this.isRepeated;
	}

	void setIsDiscarding(boolean isDiscarding) {
		this.isDiscarding = isDiscarding;
	}

	void setIsRepeated(Repeated isRepeated) {
		this.isRepeated = isRepeated;
	}

	/**
	 * As described in the {@link DslTriggerRunner} dsl triggers can be composed in tree-like structures.
	 * This method returns the children of this trigger in the tree.
	 *
	 * @return The list of child triggers.
	 */
	abstract List<DslTrigger<T, W>> getChildTriggers();

	/**
	 * Takes the specification of the {@link DslTrigger} and translates into into the most suitable implementation.
	 *
	 * @param windowSerializer the serializer for the windows created by the {@link WindowAssigner}.
	 * @param allowedLateness the allowed lateness as specified by the user.
	 * @return The implementation to be used.
	 */
	abstract DslTrigger<T, W> translate(TypeSerializer<W> windowSerializer, long allowedLateness);

	abstract <S extends State> List<StateDescriptor<S, ?>> getStateDescriptors();

	/**
	 * Determines if the trigger should fire because of a given timer.
	 *
	 * @param time The timestamp of the timer.
	 * @param isEventTimeTimer <tt>true</tt> if it is an event time timer, <tt>false</tt> if it is a processing time one.
	 * @param context A context object that can be used to register timer callbacks, check the
	 *            current event and processing time and get access to registered state.
	 */
	abstract boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, W window, DslTriggerContext context) throws Exception;

	/**
	 * Called for every element that gets added to a pane. The result of this will determine
	 * whether the pane is evaluated to emit results.
	 *
	 * @param element The element that arrived.
	 * @param timestamp The timestamp of the element that arrived.
	 * @param window The window to which the element is being added.
	 * @param context A context object that can be used to register timer callbacks, check the
	 *            current event and processing time and get access to registered state.
	 */
	abstract boolean onElement(T element, long timestamp, W window, DslTriggerContext context) throws Exception;

	/**
	 * Called after a window has fired.
	 *
	 * @param window The window that fired.
	 * @param context A context object that can be used to register timer callbacks, check the
	 *            current event and processing time and get access to registered state.
	 */
	abstract void onFire(W window, DslTriggerContext context) throws Exception;

	/**
	 * Returns true if this trigger supports merging of trigger state and can therefore
	 * be used with a
	 * {@link org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner}.
	 *
	 * <p>If this returns {@code true} you must properly implement
	 * {@link #onMerge(Window, DslTriggerContext)}
	 */
	public boolean canMerge() {
		return false;
	}

	/**
	 * Called when several windows have been merged into one window by the
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}.
	 *
	 * @param window The new window that results from the merge.
	 * @param context A context object that can be used to register timer callbacks, check the
	 *            current event and processing time and get access to registered state.
	 */
	boolean onMerge(W window, DslTriggerContext context) throws Exception {
		throw new RuntimeException("This trigger does not support merging.");
	}

	/**
	 * Clears any state that the trigger might still hold for the given window.
	 *
	 * <p>By default, this method does nothing.
	 */
	void clear(W window, DslTriggerContext context) throws Exception {}

	/**
	 * A context allowing {@link DslTrigger dsl triggers} to register timers, query the time
	 * (event and processing) and fetch trigger-related state from the state backend.
	 */
	interface DslTriggerContext extends Serializable {

		/**
		 * Sets the current invokable and the trigger context.
		 */
		void setTtriggerContext(Trigger.TriggerContext ctx, DslTriggerInvokable invokable);

		/**
		 * Returns the invokable corresponding to the child trigger at position <tt>index</tt> of the
		 * current trigger. The current is assumed to have been already set by the
		 * {@link #setTtriggerContext(Trigger.TriggerContext, DslTriggerInvokable)} method.
		 */
		<T, W extends Window> DslTriggerInvokable<T, W> getChildInvokable(int index);

		/**
		 * Returns the current processing time.
		 */
		long getCurrentProcessingTime();

		/**
		 * Returns the current watermark time.
		 */
		long getCurrentWatermark();

		/**
		 * Register a system time callback. When the current system time passes the specified
		 * time {@link Trigger#onProcessingTime(long, Window, Trigger.TriggerContext)} is called with the time specified here.
		 *
		 * @param time The time at which to invoke {@link Trigger#onProcessingTime(long, Window, Trigger.TriggerContext)}
		 */
		void registerProcessingTimeTimer(long time);

		/**
		 * Register an event-time callback. When the current watermark passes the specified
		 * time {@link Trigger#onEventTime(long, Window, Trigger.TriggerContext)} is called with the time specified here.
		 *
		 * @param time The watermark at which to invoke {@link Trigger#onEventTime(long, Window, Trigger.TriggerContext)}
		 * @see org.apache.flink.streaming.api.watermark.Watermark
		 */
		void registerEventTimeTimer(long time);

		@SuppressWarnings("unchecked")
		<S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor);

		@SuppressWarnings("unchecked")
		<S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor);
	}
}

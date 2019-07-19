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

package org.apache.flink.table.runtime.operators.window.triggers;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;

import java.io.Serializable;

/**
 * A {@code Trigger} determines when a pane of a window should be evaluated to emit the
 * results for that part of the window.
 *
 * <p>A pane is the bucket of elements that have the same key and same {@link Window}.
 * An element can be in multiple panes if it was assigned to multiple windows by the
 * {@link WindowAssigner}. These panes all have their own instance of the {@code Trigger}.
 *
 * <p>Triggers must not maintain state internally since they can be re-created or reused for
 * different keys. All necessary state should be persisted using the state abstraction
 * available on the {@link TriggerContext}.
 *
 * @param <W> The type of {@link Window Windows} on which this {@code Trigger} can operate.
 */
public abstract class Trigger<W extends Window> implements Serializable {

	private static final long serialVersionUID = -4104633972991191369L;

	/**
	 * Initialization method for the trigger. Creates states in this method.
	 * @param ctx A context object that can be used to get states.
	 */
	public abstract void open(TriggerContext ctx) throws Exception;

	/**
	 * Called for every element that gets added to a pane. The result of this will determine
	 * whether the pane is evaluated to emit results.
	 *
	 * @param element The element that arrived.
	 * @param timestamp The timestamp of the element that arrived.
	 * @param window The window to which the element is being added.
	 * @return true for firing the window, false for no action
	 */
	public abstract boolean onElement(Object element, long timestamp, W window) throws Exception;

	/**
	 * Called when a processing-time timer that was set using the trigger context fires.
	 *
	 * <p>Note: This method is not called in case the window does not contain any elements. Thus,
	 * if you return {@code PURGE} from a trigger method and you expect to do cleanup in a future
	 * invocation of a timer callback it might be wise to clean any state that you would clean
	 * in the timer callback.
	 *
	 * @param time The timestamp at which the timer fired.
	 * @param window The window for which the timer fired.
	 * @return true for firing the window, false for no action
	 */
	public abstract boolean onProcessingTime(long time, W window) throws Exception;

	/**
	 * Called when an event-time timer that was set using the trigger context fires.
	 *
	 * <p>Note: This method is not called in case the window does not contain any elements. Thus,
	 * if you return {@code PURGE} from a trigger method and you expect to do cleanup in a future
	 * invocation of a timer callback it might be wise to clean any state that you would clean
	 * in the timer callback.
	 *
	 * @param time The timestamp at which the timer fired.
	 * @param window The window for which the timer fired.
	 * @return true for firing the window, false for no action
	 */
	public abstract boolean onEventTime(long time, W window) throws Exception;

	/**
	 * Returns true if this trigger supports merging of trigger state and can therefore.
	 *
	 * <p>If this returns {@code true} you must properly implement
	 * {@link #onMerge(Window, OnMergeContext)}
	 */
	public boolean canMerge() {
		return false;
	}

	/**
	 * Called when several windows have been merged into one window by the
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}.
	 *
	 * @param window The new window that results from the merge.
	 */
	public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
		throw new UnsupportedOperationException("This trigger does not support merging.");
	}

	/**
	 * Clears any state that the trigger might still hold for the given window. This is called
	 * when a window is purged. Timers set using {@link TriggerContext#registerEventTimeTimer(long)}
	 * and {@link TriggerContext#registerProcessingTimeTimer(long)} should be deleted here as
	 * well as state acquired using {@code TriggerContext#getPartitionedState(StateDescriptor)}.
	 */
	public abstract void clear(W window) throws Exception;

	// ------------------------------------------------------------------------

	/**
	 * A context object that is given to {@link Trigger} methods to allow them to register timer
	 * callbacks and deal with state.
	 */
	public interface TriggerContext {

		/**
		 * Returns the current processing time.
		 */
		long getCurrentProcessingTime();

		/**
		 * Returns the metric group for this {@link Trigger}. This is the same metric
		 * group that would be returned from {@link RuntimeContext#getMetricGroup()} in a user
		 * function.
		 *
		 * <p>You must not call methods that create metric objects
		 * (such as {@link MetricGroup#counter(int)} multiple times but instead call once
		 * and store the metric object in a field.
		 */
		MetricGroup getMetricGroup();

		/**
		 * Returns the current watermark time.
		 */
		long getCurrentWatermark();

		/**
		 * Register a system time callback. When the current system time passes the specified
		 * time {@link Trigger#onProcessingTime(long, Window)} is called with the time specified here.
		 *
		 * @param time The time at which to invoke {@link Trigger#onProcessingTime(long, Window)}
		 */
		void registerProcessingTimeTimer(long time);

		/**
		 * Register an event-time callback. When the current watermark passes the specified
		 * time {@link Trigger#onEventTime(long, Window)} is called with the time specified here.
		 *
		 * @param time The watermark at which to invoke {@link Trigger#onEventTime(long, Window)}
		 * @see org.apache.flink.streaming.api.watermark.Watermark
		 */
		void registerEventTimeTimer(long time);

		/**
		 * Delete the processing time trigger for the given time.
		 */
		void deleteProcessingTimeTimer(long time);

		/**
		 * Delete the event-time trigger for the given time.
		 */
		void deleteEventTimeTimer(long time);

		/**
		 * Retrieves a {@link State} object that can be used to interact with
		 * fault-tolerant state that is scoped to the window and key of the current
		 * trigger invocation.
		 *
		 * @param stateDescriptor The StateDescriptor that contains the name and type of the
		 *                        state that is being accessed.
		 * @param <S>             The type of the state.
		 * @return The partitioned state object.
		 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
		 *                                       function (function is not part os a KeyedStream).
		 */
		<S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor);
	}

	/**
	 * Extension of {@link TriggerContext} that is given to
	 * {@link Trigger#onMerge(Window, OnMergeContext)}.
	 */
	public interface OnMergeContext extends TriggerContext {
		<S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor);
	}
}


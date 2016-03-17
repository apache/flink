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

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.Serializable;

/**
 * A {@code Trigger} determines when a pane of a window should be evaluated to emit the
 * results for that part of the window.
 *
 * <p>
 * A pane is the bucket of elements that have the same key (assigned by the
 * {@link org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can
 * be in multiple panes of it was assigned to multiple windows by the
 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. These panes all
 * have their own instance of the {@code Trigger}.
 *
 * <p>
 * Triggers must not maintain state internally since they can be re-created or reused for
 * different keys. All necessary state should be persisted using the state abstraction
 * available on the {@link TriggerContext}.
 *
 * @param <T> The type of elements on which this {@code Trigger} works.
 * @param <W> The type of {@link Window Windows} on which this {@code Trigger} can operate.
 */
@PublicEvolving
public abstract class Trigger<T, W extends Window> implements Serializable {
	
	private static final long serialVersionUID = -4104633972991191369L;

	/**
	 * Called for every element that gets added to a pane. The result of this will determine
	 * whether the pane is evaluated to emit results.
	 *
	 * @param element The element that arrived.
	 * @param timestamp The timestamp of the element that arrived.
	 * @param window The window to which this pane belongs.
	 * @param ctx A context object that can be used to register timer callbacks.
	 */
	public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;

	/**
	 * Called when a processing-time timer that was set using the trigger context fires.
	 *
	 * @param time The timestamp at which the timer fired.
	 * @param ctx A context object that can be used to register timer callbacks.
	 */
	public abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception;

	/**
	 * Called when an event-time timer that was set using the trigger context fires.
	 *
	 * @param time The timestamp at which the timer fired.
	 * @param ctx A context object that can be used to register timer callbacks.
	 */
	public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;

	/**
	 * Clears any state that the trigger might still hold for the given window. This is called
	 * when a window is purged. Timers set using {@link TriggerContext#registerEventTimeTimer(long)}
	 * and {@link TriggerContext#registerProcessingTimeTimer(long)} should be deleted here as
	 * well as state acquired using {@link TriggerContext#getPartitionedState(StateDescriptor)}.
	 * 
	 * <p>By default, this method does nothing.
	 */
	public void clear(W window, TriggerContext ctx) throws Exception {}

	// ------------------------------------------------------------------------
	
	/**
	 * A context object that is given to {@link Trigger} methods to allow them to register timer
	 * callbacks and deal with state.
	 */
	public interface TriggerContext {
	
		/**
		 * Register a system time callback. When the current system time passes the specified
		 * time {@link Trigger#onProcessingTime(long, Window, TriggerContext)} is called with the time specified here.
		 *
		 * @param time The time at which to invoke {@link Trigger#onProcessingTime(long, Window, TriggerContext)}
		 */
		void registerProcessingTimeTimer(long time);
	
		/**
		 * Register an event-time callback. When the current watermark passes the specified
		 * time {@link Trigger#onEventTime(long, Window, TriggerContext)} is called with the time specified here.
		 *
		 * @param time The watermark at which to invoke {@link Trigger#onEventTime(long, Window, TriggerContext)}
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
		 * Retrieves an {@link State} object that can be used to interact with
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
	
		/**
		 * Retrieves a {@link ValueState} object that can be used to interact with
		 * fault-tolerant state that is scoped to the window and key of the current
		 * trigger invocation.
		 *
		 * @param name The name of the key/value state.
		 * @param stateType The class of the type that is stored in the state. Used to generate
		 *                  serializers for managed memory and checkpointing.
		 * @param defaultState The default state value, returned when the state is accessed and
		 *                     no value has yet been set for the key. May be null.
		 *
		 * @param <S>          The type of the state.
		 * @return The partitioned state object.
		 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
		 *                                       function (function is not part os a KeyedStream).
		 */
		@Deprecated
		<S extends Serializable> ValueState<S> getKeyValueState(String name, Class<S> stateType, S defaultState);
	
	
		/**
		 * Retrieves a {@link ValueState} object that can be used to interact with
		 * fault-tolerant state that is scoped to the window and key of the current
		 * trigger invocation.
		 *
		 * @param name The name of the key/value state.
		 * @param stateType The type information for the type that is stored in the state.
		 *                  Used to create serializers for managed memory and checkpoints.
		 * @param defaultState The default state value, returned when the state is accessed and
		 *                     no value has yet been set for the key. May be null.
		 *
		 * @param <S>          The type of the state.
		 * @return The partitioned state object.
		 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
		 *                                       function (function is not part os a KeyedStream).
		 */
		@Deprecated
		<S extends Serializable> ValueState<S> getKeyValueState(String name, TypeInformation<S> stateType, S defaultState);
	}
}

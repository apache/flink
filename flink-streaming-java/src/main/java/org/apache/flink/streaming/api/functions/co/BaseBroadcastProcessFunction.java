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

package org.apache.flink.streaming.api.functions.co;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.util.OutputTag;

/**
 * The base class containing the functionality available to all broadcast process function.
 * These include the {@link BroadcastProcessFunction} and the {@link KeyedBroadcastProcessFunction}.
 */
@PublicEvolving
public abstract class BaseBroadcastProcessFunction extends AbstractRichFunction {

	private static final long serialVersionUID = -131631008887478610L;

	/**
	 * The base context available to all methods in a broadcast process function. This
	 * include {@link BroadcastProcessFunction BroadcastProcessFunctions} and
	 * {@link KeyedBroadcastProcessFunction KeyedBroadcastProcessFunctions}.
	 */
	abstract class BaseContext {

		/**
		 * Timestamp of the element currently being processed or timestamp of a firing timer.
		 *
		 * <p>This might be {@code null}, for example if the time characteristic of your program
		 * is set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
		 */
		public abstract Long timestamp();

		/**
		 * Emits a record to the side output identified by the {@link OutputTag}.
		 *
		 * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
		 * @param value The record to emit.
		 */
		public abstract <X> void output(final OutputTag<X> outputTag, final X value);

		/** Returns the current processing time. */
		public abstract long currentProcessingTime();

		/** Returns the current event-time watermark. */
		public abstract long currentWatermark();
	}

	/**
	 * A base {@link BaseContext context} available to the broadcasted stream side of
	 * a {@link org.apache.flink.streaming.api.datastream.BroadcastConnectedStream BroadcastConnectedStream}.
	 *
	 * <p>Apart from the basic functionality of a {@link BaseContext context},
	 * this also allows to get and update the elements stored in the
	 * {@link BroadcastState broadcast state}.
	 * In other words, it gives read/write access to the broadcast state.
	 */
	public abstract class Context extends BaseContext {

		/**
		 * Fetches the {@link BroadcastState} with the specified name.
		 *
		 * @param stateDescriptor the {@link MapStateDescriptor} of the state to be fetched.
		 * @return The required {@link BroadcastState broadcast state}.
		 */
		public abstract <K, V> BroadcastState<K, V> getBroadcastState(final MapStateDescriptor<K, V> stateDescriptor);
	}

	/**
	 * A {@link BaseContext context} available to the non-broadcasted stream side of
	 * a {@link org.apache.flink.streaming.api.datastream.BroadcastConnectedStream BroadcastConnectedStream}.
	 *
	 * <p>Apart from the basic functionality of a {@link BaseContext context},
	 * this also allows to get a <b>read-only</b> {@link Iterable} over the elements stored in the
	 * broadcast state.
	 */
	public abstract class ReadOnlyContext extends BaseContext {

		/**
		 * Fetches a read-only view of the broadcast state with the specified name.
		 *
		 * @param stateDescriptor the {@link MapStateDescriptor} of the state to be fetched.
		 * @return The required read-only view of the broadcast state.
		 */
		public abstract <K, V> ReadOnlyBroadcastState<K, V> getBroadcastState(final MapStateDescriptor<K, V> stateDescriptor);
	}
}

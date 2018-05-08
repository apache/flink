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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A StateDescriptor for {@link AggregatingState}.
 *
 * <p>The type internally stored in the state is the type of the {@code Accumulator} of the
 * {@code AggregateFunction}.
 *
 * @param <IN> The type of the values that are added to the state.
 * @param <ACC> The type of the accumulator (intermediate aggregation state).
 * @param <OUT> The type of the values that are returned from the state.
 */
@PublicEvolving
public class AggregatingStateDescriptor<IN, ACC, OUT> extends StateDescriptor<AggregatingState<IN, OUT>, ACC> {
	private static final long serialVersionUID = 1L;

	/** The aggregation function for the state. */
	private final AggregateFunction<IN, ACC, OUT> aggFunction;

	/**
	 * Creates a new state descriptor with the given name, function, and type.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #AggregatingStateDescriptor(String, AggregateFunction, TypeInformation)} constructor.
	 *
	 * @param name The (unique) name for the state.
	 * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
	 * @param stateType The type of the accumulator. The accumulator is stored in the state.
	 */
	public AggregatingStateDescriptor(
			String name,
			AggregateFunction<IN, ACC, OUT> aggFunction,
			Class<ACC> stateType) {

		super(name, stateType, null);
		this.aggFunction = checkNotNull(aggFunction);
	}

	/**
	 * Creates a new {@code ReducingStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
	 * @param stateType The type of the accumulator. The accumulator is stored in the state.
	 */
	public AggregatingStateDescriptor(
			String name,
			AggregateFunction<IN, ACC, OUT> aggFunction,
			TypeInformation<ACC> stateType) {

		super(name, stateType, null);
		this.aggFunction = checkNotNull(aggFunction);
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
	 * @param typeSerializer The serializer for the accumulator. The accumulator is stored in the state.
	 */
	public AggregatingStateDescriptor(
			String name,
			AggregateFunction<IN, ACC, OUT> aggFunction,
			TypeSerializer<ACC> typeSerializer) {

		super(name, typeSerializer, null);
		this.aggFunction = checkNotNull(aggFunction);
	}

	// ------------------------------------------------------------------------

	@Override
	public AggregatingState<IN, OUT> bind(StateBinder stateBinder) throws Exception {
		return stateBinder.createAggregatingState(this);
	}

	/**
	 * Returns the aggregate function to be used for the state.
	 */
	public AggregateFunction<IN, ACC, OUT> getAggregateFunction() {
		return aggFunction;
	}

	@Override
	public Type getType() {
		return Type.AGGREGATING;
	}
}

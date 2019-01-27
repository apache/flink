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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link StateDescriptor} for {@link ReducingState}. This can be used to create partitioned
 * reducing state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getReducingState(ReducingStateDescriptor)}.
 *
 * @param <T> The type of the values that can be added to the list state.
 */
@PublicEvolving
public class ReducingStateDescriptor<T> extends StateDescriptor<ReducingState<T>, T> {

	private static final long serialVersionUID = 1L;

	private final ReduceFunction<T> reduceFunction;

	/**
	 * Creates a new {@code ReducingStateDescriptor} with the given name, type, and default value.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #ReducingStateDescriptor(String, ReduceFunction, TypeInformation)} constructor.
	 *
	 * @param name The (unique) name for the state.
	 * @param reduceFunction The {@code ReduceFunction} used to aggregate the state.
	 * @param typeClass The type of the values in the state.
	 */
	public ReducingStateDescriptor(String name, ReduceFunction<T> reduceFunction, Class<T> typeClass) {
		super(name, typeClass, null);
		this.reduceFunction = checkNotNull(reduceFunction);

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of ReducingState can not be a RichFunction.");
		}
	}

	/**
	 * Creates a new {@code ReducingStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param reduceFunction The {@code ReduceFunction} used to aggregate the state.
	 * @param typeInfo The type of the values in the state.
	 */
	public ReducingStateDescriptor(String name, ReduceFunction<T> reduceFunction, TypeInformation<T> typeInfo) {
		super(name, typeInfo, null);
		this.reduceFunction = checkNotNull(reduceFunction);
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param reduceFunction The {@code ReduceFunction} used to aggregate the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 */
	public ReducingStateDescriptor(String name, ReduceFunction<T> reduceFunction, TypeSerializer<T> typeSerializer) {
		super(name, typeSerializer, null);
		this.reduceFunction = checkNotNull(reduceFunction);
	}

	// ------------------------------------------------------------------------

	@Override
	public ReducingState<T> bind(StateBinder stateBinder) throws Exception {
		return stateBinder.createReducingState(this);
	}

	/**
	 * Returns the reduce function to be used for the reducing state.
	 */
	public ReduceFunction<T> getReduceFunction() {
		return reduceFunction;
	}

	@Override
	public Type getType() {
		return Type.REDUCING;
	}
}

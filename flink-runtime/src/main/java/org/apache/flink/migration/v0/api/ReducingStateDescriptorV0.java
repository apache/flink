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

package org.apache.flink.migration.v0.api;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static java.util.Objects.requireNonNull;

/**
 * The descriptor for {@link ReducingState}s in SavepointV0.
 *
 * @param <T> The type of the values that can be added to the list state.
 */
@Deprecated
@SuppressWarnings("deprecation")
public class ReducingStateDescriptorV0<T> extends StateDescriptorV0<ReducingState<T>, T> {
	private static final long serialVersionUID = 1L;

	private final ReduceFunction<T> reduceFunction;

	/**
	 * Creates a new {@code ReducingStateDescriptorV0} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param reduceFunction The {@code ReduceFunction} used to aggregate the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 */
	public ReducingStateDescriptorV0(String name, ReduceFunction<T> reduceFunction, TypeSerializer<T> typeSerializer) {
		super(name, typeSerializer, null);
		this.reduceFunction = requireNonNull(reduceFunction);
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns the reduce function to be used for the reducing state.
	 */
	public ReduceFunction<T> getReduceFunction() {
		return reduceFunction;
	}

	@Override
	public StateDescriptor<ReducingState<T>, T> convert() {
		return new ReducingStateDescriptor<>(name, reduceFunction, serializer);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ReducingStateDescriptorV0<?> that = (ReducingStateDescriptorV0<?>) o;

		return serializer.equals(that.serializer) && name.equals(that.name);

	}

	@Override
	public int hashCode() {
		int result = serializer.hashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "FoldingStateDescriptorV0{" +
				"serializer=" + serializer +
				", reduceFunction=" + reduceFunction +
				'}';
	}
}

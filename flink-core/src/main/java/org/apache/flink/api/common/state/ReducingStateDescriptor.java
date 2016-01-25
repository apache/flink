/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static java.util.Objects.requireNonNull;

/**
 * {@link StateDescriptor} for {@link ReducingState}. This can be used to create partitioned
 * reducing state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getPartitionedState(StateDescriptor)}.
 *
 * @param <T> The type of the values that can be added to the list state.
 */
public class ReducingStateDescriptor<T> extends StateDescriptor<ReducingState<T>> {
	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> serializer;

	private final ReduceFunction<T> reduceFunction;

	/**
	 * Creates a new {@code ReducingStateDescriptor} with the given name and reduce function.
	 *
	 * @param name The (unique) name for the state.
	 * @param serializer {@link TypeSerializer} for the state values.
	 */
	public ReducingStateDescriptor(String name,
			ReduceFunction<T> reduceFunction,
			TypeSerializer<T> serializer) {
		super(requireNonNull(name));
		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of ReducingState can not be a RichFunction.");
		}
		this.serializer = requireNonNull(serializer);
		this.reduceFunction = reduceFunction;
	}

	@Override
	public ReducingState<T> bind(StateBackend stateBackend) throws Exception {
		return stateBackend.createReducingState(this);
	}

	/**
	 * Returns the {@link TypeSerializer} that can be used to serialize the value in the state.
	 */
	public TypeSerializer<T> getSerializer() {
		return serializer;
	}

	/**
	 * Returns the reduce function to be used for the reducing state.
	 */
	public ReduceFunction<T> getReduceFunction() {
		return reduceFunction;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ReducingStateDescriptor<?> that = (ReducingStateDescriptor<?>) o;

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
		return "ReducingStateDescriptor{" +
			"serializer=" + serializer +
			", reduceFunction=" + reduceFunction +
			'}';
	}
}

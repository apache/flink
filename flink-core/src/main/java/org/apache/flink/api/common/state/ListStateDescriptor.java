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

import org.apache.flink.api.common.typeutils.TypeSerializer;

import static java.util.Objects.requireNonNull;

/**
 * {@link StateDescriptor} for {@link ListState}. This can be used to create a partitioned
 * list state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getPartitionedState(StateDescriptor)}.
 *
 * @param <T> The type of the values that can be added to the list state.
 */
public class ListStateDescriptor<T> extends StateDescriptor<ListState<T>> {
	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> serializer;

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name.
	 *
	 * @param name The (unique) name for the state.
	 * @param serializer {@link TypeSerializer} for the state values.
	 */
	public ListStateDescriptor(String name, TypeSerializer<T> serializer) {
		super(requireNonNull(name));
		this.serializer = requireNonNull(serializer);
	}

	@Override
	public ListState<T> bind(StateBackend stateBackend) throws Exception {
		return stateBackend.createListState(this);
	}

	/**
	 * Returns the {@link TypeSerializer} that can be used to serialize the value in the state.
	 */
	public TypeSerializer<T> getSerializer() {
		return serializer;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ListStateDescriptor<?> that = (ListStateDescriptor<?>) o;

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
		return "ListStateDescriptor{" +
			"serializer=" + serializer +
			'}';
	}
}

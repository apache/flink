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

package org.apache.flink.migration.api.common.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.StateBinder;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * The old version of the {@link org.apache.flink.api.common.state.ListStateDescriptor}, retained for
 * serialization backwards compatibility.
 */
@Internal
@Deprecated
@SuppressWarnings("deprecation")
public class ListStateDescriptor<T> extends StateDescriptor<ListState<T>, T> {
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #ListStateDescriptor(String, TypeInformation)} constructor.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeClass The type of the values in the state.
	 */
	public ListStateDescriptor(String name, Class<T> typeClass) {
		super(name, typeClass, null);
	}

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the values in the state.
	 */
	public ListStateDescriptor(String name, TypeInformation<T> typeInfo) {
		super(name, typeInfo, null);
	}

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeSerializer The type serializer for the list values.
	 */
	public ListStateDescriptor(String name, TypeSerializer<T> typeSerializer) {
		super(name, typeSerializer, null);
	}

	// ------------------------------------------------------------------------

	@Override
	public ListState<T> bind(StateBinder stateBinder) throws Exception {
		throw new IllegalStateException("Cannot bind states with a legacy state descriptor.");
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

	@Override
	public org.apache.flink.api.common.state.StateDescriptor.Type getType() {
		return org.apache.flink.api.common.state.StateDescriptor.Type.LIST;
	}
}

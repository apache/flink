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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * The descriptor for {@link ListState}s in SavepointV0.
 */
@Deprecated
@SuppressWarnings("deprecation")
public class ListStateDescriptorV0<T> extends StateDescriptorV0<ListState<T>, T> {
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new {@code ListStateDescriptorV0} with the given name and list element type.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeSerializer The type serializer for the list values.
	 */
	public ListStateDescriptorV0(String name, TypeSerializer<T> typeSerializer) {
		super(name, typeSerializer, null);
	}

	// ------------------------------------------------------------------------

	@Override
	public ListStateDescriptor<T> convert() {
		return new ListStateDescriptor<>(name, serializer);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ListStateDescriptorV0<?> that = (ListStateDescriptorV0<?>) o;

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

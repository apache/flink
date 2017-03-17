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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * The descriptor for {@link ValueState}s in SavepointV0.
 *
 * @param <T> The type of the values that the value state can hold.
 */
@Deprecated
@SuppressWarnings("deprecation")
public class ValueStateDescriptorV0<T> extends StateDescriptorV0<ValueState<T>, T> {
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new {@code ValueStateDescriptorV0} with the given name, default value, and the specific
	 * serializer.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	public ValueStateDescriptorV0(String name, TypeSerializer<T> typeSerializer, T defaultValue) {
		super(name, typeSerializer, defaultValue);
	}

	@Override
	public StateDescriptor<ValueState<T>, T> convert() {
		return new ValueStateDescriptor<>(name, serializer, defaultValue);
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ValueStateDescriptorV0<?> that = (ValueStateDescriptorV0<?>) o;

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
		return "ValueStateDescriptorV0{" +
				"name=" + name +
				", defaultValue=" + defaultValue +
				", serializer=" + serializer +
				'}';
	}
}

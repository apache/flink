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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * {@link StateDescriptor} for {@link ValueState}. This can be used to create partitioned
 * value state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getState(ValueStateDescriptor)}.
 *
 * @param <T> The type of the values that the value state can hold.
 */
public class ValueStateDescriptor<T> extends StateDescriptor<ValueState<T>, T> {
	private static final long serialVersionUID = 1L;
	
	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name, type, and default value.
	 * 
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #ValueStateDescriptor(String, TypeInformation, Object)} constructor.
	 * 
	 * @param name The (unique) name for the state.
	 * @param typeClass The type of the values in the state.   
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	public ValueStateDescriptor(String name, Class<T> typeClass, T defaultValue) {
		super(name, typeClass, defaultValue);
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	public ValueStateDescriptor(String name, TypeInformation<T> typeInfo, T defaultValue) {
		super(name, typeInfo, defaultValue);
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name, default value, and the specific
	 * serializer.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 * @param defaultValue The default value that will be set when requesting state without setting
	 *                     a value before.
	 */
	public ValueStateDescriptor(String name, TypeSerializer<T> typeSerializer, T defaultValue) {
		super(name, typeSerializer, defaultValue);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public ValueState<T> bind(StateBackend stateBackend) throws Exception {
		return stateBackend.createValueState(this);
	}
}

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

package org.apache.flink.runtime.state.keyed;

import org.apache.flink.runtime.state.StateTransformationFunction;

import java.util.Map;

/**
 * The interface for {@link KeyedState} whose values are single-values.
 *
 * @param <K> The type of the keys in the state.
 * @param <V> The type of the keys in the state.
 */
public interface KeyedValueState<K, V> extends KeyedState<K, V> {

	/**
	 * Associates the given value with the given key in the state. If the key
	 * is already associated with a value, the value will be replaced with the
	 * given value.
	 *
	 * @param key The key with which the given value is to be associated.
	 * @param value The value to be associated with the given key.
	 */
	void put(K key, V value);

	/**
	 * Associates all the keys in the given map with corresponding values.
	 * The association of these keys are atomic, exceptions will be thrown if
	 * some of the keys fail to be associated.
	 *
	 * @param pairs The pairs to be added into the state.
	 */
	void putAll(Map<? extends K, ? extends V> pairs);

	/**
	 * Applies the given {@link StateTransformationFunction} to the state (1st input argument), using the given value as
	 * second input argument. The result of {@link StateTransformationFunction#apply(Object, Object)} is then stored as
	 * the new state. This function is basically an optimization for get-update-put pattern.
	 *
	 * @param key            the key.
	 * @param value          the value to use in transforming the state.
	 * @param transformation the transformation function.
	 */
	<T> void transform(K key, T value, StateTransformationFunction<V, T> transformation);
}

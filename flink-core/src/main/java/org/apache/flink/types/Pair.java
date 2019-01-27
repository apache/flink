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

package org.apache.flink.types;

/**
 * Base class for key-value pairs. Each pair's key is immutable, but the value
 * associated with the key can be modified.
 *
 * @param <K> The type of the key in the pair.
 * @param <V> The type of the value in the pair.
 */
public interface Pair<K, V> {

	/**
	 * Returns the key corresponding to the pair.
	 *
	 * @return The key corresponding to the pair.
	 */
	K getKey();

	/**
	 * Returns the value corresponding to the pair.
	 *
	 * @return The value corresponding to the pair.
	 */
	V getValue();

	/**
	 * Replaces the value corresponding to the pair with the given value.
	 * (optional).
	 *
	 * @param value The new value to be associated the pair's key.
	 * @return The old value corresponding to the pair.
	 * @throws UnsupportedOperationException if the operation is not supported.
	 */
	V setValue(V value);
}

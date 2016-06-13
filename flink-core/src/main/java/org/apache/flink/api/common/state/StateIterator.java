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

/**
 * Iterator for iterating over the state for all keys in a parallel partition.
 *
 * <p>Use {@link #advance()} to progress and {@link #key()} and {@link #state()} to access
 * the current key and state, respectively. Initially, the iterator is not in any read position,
 * you have to call {@link #advance()} first, before trying to access the key and/or state.
 *
 * @param <K> The type of the Key.
 * @param <S> The type of {@link State} that we iterate over.
 */
public interface StateIterator<K, S> {

	/**
	 * Returns the key of the current state.
	 */
	K key();

	/**
	 * Returns the current state.
	 */
	S state() throws Exception;

	/**
	 * Delete the state for the current key.
	 * @throws Exception
	 */
	void delete() throws Exception;

	/**
	 * Advances the iterator to the next key/state. Returns {@code false} if no more keys
	 * are available.
	 */
	boolean advance() throws Exception;
}

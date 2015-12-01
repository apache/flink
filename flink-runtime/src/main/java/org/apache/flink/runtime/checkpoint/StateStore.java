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

package org.apache.flink.runtime.checkpoint;

import java.io.Serializable;

/**
 * A simple {@link org.apache.flink.runtime.state.StateBackend} backend object
 * store interface.
 *
 * <p><strong>Important:</strong> This does not work with user-specified classes.
 *
 * @param <T> The type of objects to store.
 */
public interface StateStore<T extends Serializable> {

	/**
	 * Puts an object into the store and returns the (logical) path to it.
	 *
	 * <p>The stored state can be retrieved or discarded by specifying the returned path.
	 *
	 * @param state The object to store.
	 * @return The path to retrieve or discard the object again.
	 */
	String putState(T state) throws Exception;

	/**
	 * Returns the object identified by the given (logical) path.
	 *
	 * @param path The path of the object to retrieve.
	 * @return The object stored under the specified path.
	 * @throws IllegalArgumentException If the path does not identify a valid object.
	 */
	T getState(String path) throws Exception;

	/**
	 * Disposes the state identified by the given (logical) path.
	 *
	 * @param path The path of the object to discard.
	 * @throws IllegalArgumentException If the path does not identify a valid object.
	 */
	void disposeState(String path) throws Exception;

}

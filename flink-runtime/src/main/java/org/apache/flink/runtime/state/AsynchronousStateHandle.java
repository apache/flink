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

package org.apache.flink.runtime.state;

/**
 * {@link StateHandle} that can asynchronously materialize the state that it represents. Instead
 * of representing a materialized handle to state this would normally hold the (immutable) state
 * internally and can materialize it if requested.
 */
public abstract class AsynchronousStateHandle<T> implements StateHandle<T> {
	private static final long serialVersionUID = 1L;

	/**
	 * Materializes the state held by this {@code AsynchronousStateHandle}.
	 */
	public abstract StateHandle<T> materialize() throws Exception;

	@Override
	public final T getState(ClassLoader userCodeClassLoader) throws Exception {
		throw new UnsupportedOperationException("This must not be called. This is likely an internal bug.");
	}

	@Override
	public final void discardState() throws Exception {
		throw new UnsupportedOperationException("This must not be called. This is likely an internal bug.");
	}
}

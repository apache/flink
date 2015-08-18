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

import java.io.Serializable;

/**
 * A StateHandle that includes the operator states directly.
 */
public class LocalStateHandle<T extends Serializable> implements StateHandle<T> {

	private static final long serialVersionUID = 2093619217898039610L;

	private final T state;

	public LocalStateHandle(T state) {
		this.state = state;
	}

	@Override
	public T getState(ClassLoader userCodeClassLoader) {
		// The object has been deserialized correctly before
		return state;
	}

	@Override
	public void discardState() throws Exception {
	}

	public static class LocalStateHandleProvider<R extends Serializable> implements
			StateHandleProvider<R> {

		private static final long serialVersionUID = 4665419208932921425L;

		@Override
		public LocalStateHandle<R> createStateHandle(R state) {
			return new LocalStateHandle<R>(state);
		}

	}
}

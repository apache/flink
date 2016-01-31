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

/**
 * Simple wrapper around the state store for savepoints.
 */
public class SavepointStore implements StateStore<Savepoint> {

	private final StateStore<Savepoint> stateStore;

	public SavepointStore(StateStore<Savepoint> stateStore) {
		this.stateStore = stateStore;
	}

	public void start() {
	}

	public void stop() {
		if (stateStore instanceof HeapStateStore) {
			HeapStateStore<Savepoint> heapStateStore = (HeapStateStore<Savepoint>) stateStore;

			for (Savepoint savepoint : heapStateStore.getAll()) {
				savepoint.getCompletedCheckpoint().discard(ClassLoader.getSystemClassLoader());
			}

			heapStateStore.clearAll();
		}
	}

	@Override
	public String putState(Savepoint state) throws Exception {
		return stateStore.putState(state);
	}

	@Override
	public Savepoint getState(String path) throws Exception {
		return stateStore.getState(path);
	}

	@Override
	public void disposeState(String path) throws Exception {
		stateStore.disposeState(path);
	}

	StateStore<Savepoint> getStateStore() {
		return stateStore;
	}
}

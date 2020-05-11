/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Track the {@link AsyncCheckpointRunnable}s in {@link SubtaskCheckpointCoordinator}.
 */
@Internal
public class AsyncCheckpointRunnables implements Closeable {
	private final Map<Long, AsyncCheckpointRunnable> checkpoints;
	/** Lock that guards state of this registry. **/
	private final Object lock;

	public AsyncCheckpointRunnables() {
		this.checkpoints = new HashMap<>();
		this.lock = new Object();
	}

	public void registerAsyncCheckpointRunnable(long checkpointId, AsyncCheckpointRunnable asyncCheckpointRunnable) {
		synchronized (lock) {
			checkpoints.put(checkpointId, asyncCheckpointRunnable);
		}
	}

	public boolean unregisterAsyncCheckpointRunnable(long checkpointId) {
		synchronized (lock) {
			return checkpoints.remove(checkpointId) != null;
		}
	}

	/**
	 * Cancel the async checkpoint runnable with given checkpoint id.
	 * If given checkpoint id is not registered, return false, otherwise return true.
	 */
	public boolean cancelAsyncCheckpointRunnable(long checkpointId) {
		AsyncCheckpointRunnable asyncCheckpointRunnable;
		synchronized (lock) {
			asyncCheckpointRunnable = checkpoints.remove(checkpointId);
		}
		IOUtils.closeQuietly(asyncCheckpointRunnable);
		return asyncCheckpointRunnable != null;
	}

	@VisibleForTesting
	int getAsyncCheckpointRunnableSize() {
		return checkpoints.size();
	}

	@Override
	public void close() throws IOException {
		List<AsyncCheckpointRunnable> asyncCheckpointRunnables;
		synchronized (lock) {
			asyncCheckpointRunnables = new ArrayList<>(checkpoints.values());
			checkpoints.clear();
		}
		IOUtils.closeAllQuietly(asyncCheckpointRunnables);
	}
}

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

package org.apache.flink.core.testutils;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;

/**
 * An executor that does not immediately execute a Runnable, but only executes it
 * upon an explicit trigger.
 *
 * <p>This executor can be used in concurrent tests to control when certain asynchronous
 * actions should happen.
 */
public class ManuallyTriggeredDirectExecutor implements Executor {

	private final ArrayDeque<Runnable> queuedRunnables = new ArrayDeque<>();

	@Override
	public void execute(@Nonnull Runnable command) {
		synchronized (queuedRunnables) {
			queuedRunnables.addLast(command);
		}
	}

	/**
	 * Triggers the next queued runnable and executes it synchronously.
	 * This method throws an exception if no Runnable is currently queued.
	 */
	public void trigger() {
		final Runnable next;

		synchronized (queuedRunnables) {
			next = queuedRunnables.removeFirst();
		}

		if (next != null) {
			next.run();
		}
		else {
			throw new IllegalStateException("No runnable available");
		}
	}

	/**
	 * Gets the number of Runnables currently queued.
	 */
	public int numQueuedRunnables() {
		synchronized (queuedRunnables) {
			return queuedRunnables.size();
		}
	}
}

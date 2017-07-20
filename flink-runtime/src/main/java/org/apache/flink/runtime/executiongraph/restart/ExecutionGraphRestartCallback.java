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

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link RestartCallback} that abstracts restart calls on an {@link ExecutionGraph}.
 *
 * <p>This callback implementation is one-shot; it can only be used once.
 */
public class ExecutionGraphRestartCallback implements RestartCallback {

	/** The ExecutionGraph to restart. */
	private final ExecutionGraph execGraph;

	/** Atomic flag to make sure this is used only once. */
	private final AtomicBoolean used;

	/** The globalModVersion that the ExecutionGraph needs to have for the restart to go through. */
	private final long expectedGlobalModVersion;

	/**
	 * Creates a new ExecutionGraphRestartCallback.
	 *
	 * @param execGraph The ExecutionGraph to restart
	 * @param expectedGlobalModVersion  The globalModVersion that the ExecutionGraph needs to have
	 *                                  for the restart to go through
	 */
	public ExecutionGraphRestartCallback(ExecutionGraph execGraph, long expectedGlobalModVersion) {
		this.execGraph = checkNotNull(execGraph);
		this.used = new AtomicBoolean(false);
		this.expectedGlobalModVersion = expectedGlobalModVersion;
	}

	@Override
	public void triggerFullRecovery() {
		if (used.compareAndSet(false, true)) {
			execGraph.restart(expectedGlobalModVersion);
		}
	}
}

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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Container for returning the {@link ArchivedExecutionGraph} and a flag whether the initialization has failed.
 * For initialization failures, the throwable is also attached, to avoid deserializing it from the ArchivedExecutionGraph.
 */
final class DispatcherJobResult {

	private final ArchivedExecutionGraph archivedExecutionGraph;

	// if the throwable field is set, the job failed during initialization.
	@Nullable
	private final Throwable initializationFailure;

	private DispatcherJobResult(ArchivedExecutionGraph archivedExecutionGraph, @Nullable Throwable throwable) {
		this.archivedExecutionGraph = archivedExecutionGraph;
		this.initializationFailure = throwable;
	}

	public boolean isInitializationFailure() {
		return initializationFailure != null;
	}

	public ArchivedExecutionGraph getArchivedExecutionGraph() {
		return archivedExecutionGraph;
	}

	/**
	 * @throws IllegalStateException if this DispatcherJobResult is a successful initialization.
	 */
	public Throwable getInitializationFailure() {
		Preconditions.checkState(isInitializationFailure(), "This DispatcherJobResult does not represent a failed initialization.");
		return initializationFailure;
	}

	public static DispatcherJobResult forInitializationFailure(ArchivedExecutionGraph archivedExecutionGraph, Throwable throwable) {
		return new DispatcherJobResult(archivedExecutionGraph, throwable);
	}

	public static DispatcherJobResult forSuccess(ArchivedExecutionGraph archivedExecutionGraph) {
		return new DispatcherJobResult(archivedExecutionGraph, null);
	}
}

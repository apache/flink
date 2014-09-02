/**
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


package org.apache.flink.runtime.profiling.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.execution.ExecutionListener;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.RuntimeEnvironment;
import org.apache.flink.runtime.executiongraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobID;

public class EnvironmentListenerImpl implements ExecutionListener {

	private static final Log LOG = LogFactory.getLog(EnvironmentListenerImpl.class);

	private final TaskManagerProfilerImpl taskManagerProfiler;

	private final RuntimeEnvironment environment;

	public EnvironmentListenerImpl(final TaskManagerProfilerImpl taskManagerProfiler,
			final RuntimeEnvironment environment) {

		this.taskManagerProfiler = taskManagerProfiler;
		this.environment = environment;
	}


	@Override
	public void executionStateChanged(final JobID jobID, final ExecutionVertexID vertexID,
			final ExecutionState newExecutionState, final String optionalMessage) {

		switch (newExecutionState) {
		case RUNNING:
			this.taskManagerProfiler.registerMainThreadForCPUProfiling(this.environment,
				this.environment.getExecutingThread(), vertexID);
			break;
		case FINISHING:
		case FINISHED:
		case CANCELING:
		case CANCELED:
		case FAILED:
			this.taskManagerProfiler.unregisterMainThreadFromCPUProfiling(this.environment,
				this.environment.getExecutingThread());
			break;
		default:
			LOG.error("Unexpected state transition to " + newExecutionState + " for vertex " + vertexID);
			break;
		}
	}


	@Override
	public void userThreadFinished(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {

		// Make sure the user thread is not the task's main thread
		if (this.environment.getExecutingThread() == userThread) {
			return;
		}

		this.taskManagerProfiler.unregisterUserThreadFromCPUProfiling(this.environment, userThread);
	}


	@Override
	public void userThreadStarted(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {

		// Make sure the user thread is not the task's main thread
		if (this.environment.getExecutingThread() == userThread) {
			return;
		}

		this.taskManagerProfiler.registerUserThreadForCPUProfiling(this.environment, userThread);
	}


	@Override
	public int getPriority() {

		return 1;
	}
}

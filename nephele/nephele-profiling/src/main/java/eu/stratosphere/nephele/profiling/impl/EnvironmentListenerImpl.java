/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

public class EnvironmentListenerImpl implements ExecutionListener {

	private static final Log LOG = LogFactory.getLog(EnvironmentListenerImpl.class);

	private final TaskManagerProfilerImpl taskManagerProfiler;

	private final RuntimeEnvironment environment;

	public EnvironmentListenerImpl(final TaskManagerProfilerImpl taskManagerProfiler,
			final RuntimeEnvironment environment) {

		this.taskManagerProfiler = taskManagerProfiler;
		this.environment = environment;
	}

	/**
	 * {@inheritDoc}
	 */
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {

		// Make sure the user thread is not the task's main thread
		if (this.environment.getExecutingThread() == userThread) {
			return;
		}

		this.taskManagerProfiler.unregisterUserThreadFromCPUProfiling(this.environment, userThread);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {

		// Make sure the user thread is not the task's main thread
		if (this.environment.getExecutingThread() == userThread) {
			return;
		}

		this.taskManagerProfiler.registerUserThreadForCPUProfiling(this.environment, userThread);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialExecutionResourcesExhausted(final JobID jobID, final ExecutionVertexID vertexID,
			final ResourceUtilizationSnapshot resourceUtilizationSnapshot) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getPriority() {

		return 1;
	}
}

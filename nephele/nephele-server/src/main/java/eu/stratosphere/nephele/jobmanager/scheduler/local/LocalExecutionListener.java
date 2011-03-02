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

package eu.stratosphere.nephele.jobmanager.scheduler.local;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.jobgraph.JobStatus;

/**
 * This is a wrapper class for the {@link LocalScheduler} to receive
 * notifications about state changes of vertices belonging
 * to scheduled jobs.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class LocalExecutionListener implements ExecutionListener {

	/**
	 * The instance of the {@link LocalScheduler}.
	 */
	private final LocalScheduler localScheduler;

	/**
	 * The {@link ExecutionVertex} this wrapper object belongs to.
	 */
	private final ExecutionVertex executionVertex;

	/**
	 * Constructs a new wrapper object for the given {@link ExecutionVertex}.
	 * 
	 * @param localScheduler
	 *        the instance of the {@link LocalScheduler}
	 * @param executionVertex
	 *        the {@link ExecutionVertex} the received notification refer to
	 */
	public LocalExecutionListener(LocalScheduler localScheduler, ExecutionVertex executionVertex) {
		this.localScheduler = localScheduler;
		this.executionVertex = executionVertex;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void executionStateChanged(Environment ee, ExecutionState newExecutionState, String optionalMessage) {

		final ExecutionGraph eg = this.executionVertex.getExecutionGraph();

		if (newExecutionState == ExecutionState.FINISHED || newExecutionState == ExecutionState.CANCELED
			|| newExecutionState == ExecutionState.FAILED) {
			// Check if instance can be released
			this.localScheduler.checkAndReleaseAllocatedResource(eg, this.executionVertex.getAllocatedResource());
		}

		// In case of an error, check if vertex can be rescheduled
		if (newExecutionState == ExecutionState.FAILED) {
			if (this.executionVertex.hasRetriesLeft()) {
				// Reschedule vertex
				this.executionVertex.setExecutionState(ExecutionState.SCHEDULED);
			}
		}

		final ExecutionGraph executionGraph = this.executionVertex.getExecutionGraph();
		final JobStatus jobStatus = executionGraph.getJobStatus();

		if (jobStatus == JobStatus.FAILED || jobStatus == JobStatus.FINISHED || jobStatus == JobStatus.CANCELLED) {
			this.localScheduler.removeJobFromSchedule(executionGraph);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(Environment ee, Thread userThread) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(Environment ee, Thread userThread) {
		// Nothing to do here
	}

}

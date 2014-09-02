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


package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.execution.ExecutionListener;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGroupVertex;
import org.apache.flink.runtime.executiongraph.ExecutionPipeline;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertexID;
import org.apache.flink.runtime.executiongraph.InternalJobStatus;
import org.apache.flink.runtime.jobgraph.JobID;

public class DefaultExecutionListener implements ExecutionListener {

	/**
	 * The instance of the {@link org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler}.
	 */
	private final DefaultScheduler scheduler;

	/**
	 * The {@link ExecutionVertex} this wrapper object belongs to.
	 */
	private final ExecutionVertex executionVertex;

	/**
	 * Constructs a new wrapper object for the given {@link ExecutionVertex}.
	 * 
	 * @param scheduler
	 *        the instance of the {@link DefaultScheduler}
	 * @param executionVertex
	 *        the {@link ExecutionVertex} the received notification refer to
	 */
	public DefaultExecutionListener(final DefaultScheduler scheduler, final ExecutionVertex executionVertex) {
		this.scheduler = scheduler;
		this.executionVertex = executionVertex;
	}


	@Override
	public void executionStateChanged(final JobID jobID, final ExecutionVertexID vertexID,
			final ExecutionState newExecutionState, final String optionalMessage) {

		final ExecutionGraph eg = this.executionVertex.getExecutionGraph();

		// Check if we can deploy a new pipeline.
		if (newExecutionState == ExecutionState.FINISHING) {

			final ExecutionPipeline pipeline = this.executionVertex.getExecutionPipeline();
			if (!pipeline.isFinishing()) {
				// Some tasks of the pipeline are still running
				return;
			}

			// Find another vertex in the group which is still in SCHEDULED state and get its pipeline.
			final ExecutionGroupVertex groupVertex = this.executionVertex.getGroupVertex();
			for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers(); ++i) {
				final ExecutionVertex groupMember = groupVertex.getGroupMember(i);
				if (groupMember.compareAndUpdateExecutionState(ExecutionState.SCHEDULED, ExecutionState.ASSIGNED)) {

					final ExecutionPipeline pipelineToBeDeployed = groupMember.getExecutionPipeline();
					pipelineToBeDeployed.setAllocatedResource(this.executionVertex.getAllocatedResource());
					pipelineToBeDeployed.updateExecutionState(ExecutionState.ASSIGNED);

					this.scheduler.deployAssignedPipeline(pipelineToBeDeployed);
					return;
				}
			}
		}

		if (newExecutionState == ExecutionState.CANCELED || newExecutionState == ExecutionState.FINISHED) {

			synchronized (eg) {

				if (this.scheduler.getVerticesToBeRestarted().remove(this.executionVertex.getID()) != null) {

					if (eg.getJobStatus() == InternalJobStatus.FAILING) {
						return;
					}

					this.executionVertex.updateExecutionState(ExecutionState.ASSIGNED, "Restart as part of recovery");

					// Run through the deployment procedure
					this.scheduler.deployAssignedVertices(this.executionVertex);
					return;
				}
			}
		}

		if (newExecutionState == ExecutionState.FINISHED || newExecutionState == ExecutionState.CANCELED
			|| newExecutionState == ExecutionState.FAILED) {
			// Check if instance can be released
			this.scheduler.checkAndReleaseAllocatedResource(eg, this.executionVertex.getAllocatedResource());
		}
	}


	@Override
	public void userThreadFinished(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
		// Nothing to do here
	}


	@Override
	public void userThreadStarted(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
		// Nothing to do here
	}


	@Override
	public int getPriority() {

		return 0;
	}
}

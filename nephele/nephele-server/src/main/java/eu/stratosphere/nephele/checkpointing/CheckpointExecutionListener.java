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

package eu.stratosphere.nephele.checkpointing;

import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class implements a {@link ExecutionListener} designed to receive notifications about the initial resource
 * exhaustion of a task. The notification is then forwarded to the {@link CheckpointDecisionCoordinator}.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class CheckpointExecutionListener implements ExecutionListener {

	/**
	 * The decision coordinator that will receive notifications about a vertex's resource exhaustion.
	 */
	private final CheckpointDecisionCoordinator decisionCoordinator;

	/**
	 * The execution vertex this listener has been created for.
	 */
	private final ExecutionVertex executionVertex;

	/**
	 * Constructs a new checkpoint execution listener.
	 * 
	 * @param decisionCoordinator
	 *        the decision coordinator
	 * @param executionVertex
	 *        the execution vertex this listener is created for
	 */
	CheckpointExecutionListener(final CheckpointDecisionCoordinator decisionCoordinator,
			final ExecutionVertex executionVertex) {

		if (decisionCoordinator == null) {
			throw new IllegalArgumentException("Argument decisionCoordinator must not be null");
		}

		if (executionVertex == null) {
			throw new IllegalArgumentException("Argument executionVertex must not be null");
		}

		this.decisionCoordinator = decisionCoordinator;
		this.executionVertex = executionVertex;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void executionStateChanged(final JobID jobID, final ExecutionVertexID vertexID,
			final ExecutionState newExecutionState, final String optionalMessage) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
		// Nothing to do here

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialExecutionResourcesExhausted(final JobID jobID, final ExecutionVertexID vertexID,
			final ResourceUtilizationSnapshot resourceUtilizationSnapshot) {

		// Trigger checkpoint decision
		this.decisionCoordinator.checkpointDecisionRequired(this.executionVertex, resourceUtilizationSnapshot);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getPriority() {

		return 2;
	}

}

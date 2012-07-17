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
package eu.stratosphere.pact.testing;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;

import eu.stratosphere.nephele.deployment.TaskDeploymentDescriptor;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.DeploymentManager;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.util.SerializableArrayList;

/**
 * @author Arvid Heise
 */
public class MockDeployManager implements DeploymentManager {
	public final static MockDeployManager INSTANCE = new MockDeployManager();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deploy(final JobID jobID, final AbstractInstance instance,
			final List<ExecutionVertex> verticesToBeDeployed) {
		// final Iterator<ExecutionVertex> it = verticesToBeDeployed.iterator();
		// while (it.hasNext()) {
		//
		// final ExecutionVertex executionVertex = it.next();
		//
		// ExecutionExceptionHandler executionListener = new ExecutionExceptionHandler(executionVertex);
		// final Environment environment = executionVertex.getEnvironment();
		// // environment.setExecutionObserver(executionListener);
		// environment.setInputSplitProvider(new MockInputSplitProvider(executionVertex));
		// this.errorHandlers.add(executionListener);
		//
		// final TaskSubmissionResult submissionResult = executionVertex
		// .startTask();
		//
		// if (submissionResult.getReturnCode() == AbstractTaskResult.ReturnCode.ERROR)
		// Assert.fail(submissionResult.getDescription() + " @ " + executionVertex);
		//
		// executionVertex.updateExecutionState(ExecutionState.STARTING, null);
		// }
		//

		for (final ExecutionVertex vertex : verticesToBeDeployed)
			vertex.updateExecutionState(ExecutionState.STARTING, null);

		// Create a new runnable and pass it the executor service
		final Runnable deploymentRunnable = new Runnable() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {
				final List<TaskDeploymentDescriptor> submissionList =
					new SerializableArrayList<TaskDeploymentDescriptor>();

				// Check the consistency of the call
				for (final ExecutionVertex vertex : verticesToBeDeployed) {
					// RuntimeEnvironment environment = vertex.getEnvironment();
					// environment.setExecutionObserver(new MockInputSplitProvider(vertex));

					submissionList.add(vertex.constructDeploymentDescriptor());

					// new TaskDeploymentDescriptor(vertex.getID(), environment, vertex
					// .getExecutionGraph().getJobConfiguration(), CheckpointState.NONE, vertex
					// .constructInitialActiveOutputChannelsSet()));
				}

				List<TaskSubmissionResult> submissionResultList = null;

				try {
					submissionResultList = instance.submitTasks(submissionList);
				} catch (final IOException ioe) {
					for (final ExecutionVertex vertex : verticesToBeDeployed)
						Assert.fail(ioe.getMessage() + " @ " + vertex);
					return;
				}

				int count = 0;
				for (final TaskSubmissionResult tsr : submissionResultList) {

					ExecutionVertex vertex = verticesToBeDeployed.get(count++);

					if (tsr.getReturnCode() != AbstractTaskResult.ReturnCode.SUCCESS)
						// Change the execution state to failed and let the scheduler deal with the rest
						vertex.updateExecutionState(ExecutionState.FAILED, tsr.getDescription());
				}
			}
		};

		ConcurrentUtil.invokeLater(deploymentRunnable);
	}

}

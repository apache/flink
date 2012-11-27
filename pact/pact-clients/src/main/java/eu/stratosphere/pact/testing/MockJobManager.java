/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.Iterator;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.executiongraph.JobStatusListener;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;

/**
 * @author Arvid Heise
 */
public class MockJobManager implements JobStatusListener {
	public static final MockJobManager INSTANCE = new MockJobManager();

	@Override
	public void jobStatusHasChanged(ExecutionGraph executionGraph, InternalJobStatus newJobStatus,
			String optionalMessage) {
		// System.out.println("job graph " + executionGraph.getJobID() + " -> " + newJobStatus + "; " +
		// StringUtils.stringifyException(new Throwable()));

		if (newJobStatus == InternalJobStatus.CANCELING || newJobStatus == InternalJobStatus.FAILING)
			// Cancel all remaining tasks
			try {
				this.cancelJob(executionGraph);
			} catch (InterruptedException ie) {
				return;
			}

		if (newJobStatus == InternalJobStatus.FINISHED || newJobStatus == InternalJobStatus.CANCELED
			|| newJobStatus == InternalJobStatus.FAILED)
			MockTaskManager.INSTANCE.cleanupJob(executionGraph);
	};

	/**
	 * Cancels all the tasks in the current and upper stages of the
	 * given execution graph.
	 * 
	 * @param eg
	 *        the execution graph representing the job to cancel.
	 * @return <code>null</code> no error occurred during the cancel attempt,
	 *         otherwise the returned object will describe the error
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 */
	private TaskCancelResult cancelJob(final ExecutionGraph eg) throws InterruptedException {

		TaskCancelResult errorResult = null;

		/**
		 * Cancel all nodes in the current and upper execution stages.
		 */
		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, eg.getIndexOfCurrentExecutionStage(),
			false, true);
		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();
			final TaskCancelResult result = vertex.cancelTask();
			if (result.getReturnCode() != AbstractTaskResult.ReturnCode.SUCCESS)
				errorResult = result;
		}

		return errorResult;
	}

	@Override
	public int getPriority() {
		return 0;
	}

}

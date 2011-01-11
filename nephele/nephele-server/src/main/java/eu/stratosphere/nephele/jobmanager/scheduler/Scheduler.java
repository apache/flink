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

package eu.stratosphere.nephele.jobmanager.scheduler;

import java.util.Set;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This scheduler interface must be implemented by a scheduler implementations
 * for Nephele. The interface defines the fundamental methods for scheduling and
 * removing jobs. While Nephele's {@link JobManager} is responsible for requesting
 * the required instances for the job at the {@link InstanceManager}, the scheduler
 * is in charge of assigning the individual tasks to the
 * 
 * @author warneke
 */
public interface Scheduler extends InstanceListener {

	/**
	 * Adds a job represented by an {@link ExecutionGraph} object to the scheduler. The job is then executed according
	 * to the strategies of the concrete scheduler implementation.
	 * 
	 * @param executionGraph
	 *        the job to be added to the scheduler
	 * @throws SchedulingException
	 *         thrown if an error occurs and the scheduler does not accept the new job
	 */
	void schedulJob(ExecutionGraph executionGraph) throws SchedulingException;

	/**
	 * Returns a (possibly empty) set of vertices which are ready to be executed.
	 * 
	 * @return a (possibly empty) set of vertices which are ready to be executed
	 */
	Set<ExecutionVertex> getVerticesReadyToBeExecuted();

	/**
	 * Returns the execution graph which is associated with the given job ID.
	 * 
	 * @param jobID
	 *        the job ID to search the execution graph for
	 * @return the execution graph which belongs to the given job ID or <code>null</code if no such execution graph
	 *         exists
	 */
	ExecutionGraph getExecutionGraphByID(JobID jobID);

	/**
	 * Returns the {@link InstanceManager} object which is used by the current scheduler.
	 * 
	 * @return the {@link InstanceManager} object which is used by the current scheduler
	 */
	InstanceManager getInstanceManager();

	// void removeJob(JobID jobID);

	/**
	 * Shuts the scheduler down. After shut down no jobs can be added to the scheduler.
	 */
	void shutdown();
}

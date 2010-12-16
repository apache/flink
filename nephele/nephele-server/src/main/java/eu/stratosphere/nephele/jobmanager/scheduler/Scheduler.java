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

	void schedulJob(ExecutionGraph executionGraph) throws SchedulingException;

	Set<ExecutionVertex> getVerticesReadyToBeExecuted();

	ExecutionGraph getExecutionGraphByID(JobID jobID);

	InstanceManager getInstanceManager();

	// void removeJob(JobID jobID);

	void shutdown();
}

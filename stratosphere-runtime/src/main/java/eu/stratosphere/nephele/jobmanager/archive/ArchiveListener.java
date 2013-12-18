/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobmanager.archive;

import java.util.List;

import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * Interface used to implement Archivists, that store old jobmanager information discarded by the EventCollector.
 * Archivists can decide how to store the data (memory, database, files...)
 */
public interface ArchiveListener {
	
	/**
	 * Stores event in archive
	 * 
	 * @param jobId
	 * @param event
	 */
	void archiveEvent(JobID jobId, AbstractEvent event);
	
	/**
	 * Stores old job in archive
	 * 
	 * @param jobId
	 * @param event
	 */
	void archiveJobevent(JobID jobId, RecentJobEvent event);
	
	/**
	 * Stores old ManagementGraph in archive
	 * 
	 * @param jobId
	 * @param graph
	 */
	void archiveManagementGraph(JobID jobId, ManagementGraph graph);
	
	/**
	 * Stores old NetworkTopology in Archive
	 * 
	 * @param jobId
	 * @param topology
	 */
	void archiveNetworkTopology(JobID jobId, NetworkTopology topology);
	
	/**
	 * Get all archived Jobs
	 * 
	 * @return
	 */
	List<RecentJobEvent> getJobs();
	
	/**
	 * Return archived job
	 * 
	 * @param JobId
	 * @return
	 */
	RecentJobEvent getJob(JobID JobId);
	
	/**
	 * Get archived ManagementGraph for a job
	 * 
	 * @param jobID
	 * @return
	 */
	ManagementGraph getManagementGraph(JobID jobID);
	
	/**
	 * Get all archived Events for a job
	 * 
	 * @param jobID
	 * @return
	 */
	List<AbstractEvent> getEvents(JobID jobID);
	
	/**
	 * Returns the time when the status of the given job changed to jobStatus
	 * 
	 * @param jobID
	 * @param jobStatus
	 * @return
	 */
	long getJobTime(JobID jobID, JobStatus jobStatus);
	
	/**
	 * returns the time, when the status of the given vertex changed to executionState
	 * 
	 * @param jobID
	 * @param jobVertexID
	 * @param executionState
	 * @return
	 */
	long getVertexTime(JobID jobID, ManagementVertexID jobVertexID, ExecutionState executionState);
}

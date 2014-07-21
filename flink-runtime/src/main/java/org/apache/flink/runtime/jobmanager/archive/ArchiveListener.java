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

package org.apache.flink.runtime.jobmanager.archive;

import java.util.List;

import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.RecentJobEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.managementgraph.ManagementGraph;
import org.apache.flink.runtime.managementgraph.ManagementVertexID;
import org.apache.flink.runtime.topology.NetworkTopology;

/**
 * Interface used to implement Archivists, that store old job manager
 * information discarded by the EventCollector. Archivists can decide how to
 * store the data (memory, database, files...)
 */
public interface ArchiveListener {

	/**
	 * Stores an {@link AbstractEvent} in the archive.
	 * 
	 * @param jobId
	 *            the ID of the job the event belongs to
	 * @param event
	 *            the event to store
	 */
	void archiveEvent(JobID jobId, AbstractEvent event);

	/**
	 * Stores an {@link RecentJobEvent} in the archive.
	 * 
	 * @param jobId
	 *            the ID of the job the vent belongs to
	 * @param event
	 *            the event to store
	 */
	void archiveJobEvent(JobID jobId, RecentJobEvent event);

	/**
	 * Stores an {@link ManagementGraph} in the archive.
	 * 
	 * @param jobId
	 *            the ID of the job the management graph belongs to
	 * @param graph
	 *            the management graph to store
	 */
	void archiveManagementGraph(JobID jobId, ManagementGraph graph);

	/**
	 * Stores an {@link NetworkTopology} in the archive.
	 * 
	 * @param jobId
	 *            the ID of the job the network topology belongs to
	 * @param topology
	 *            the network topology to store
	 */
	void archiveNetworkTopology(JobID jobId, NetworkTopology topology);

	/**
	 * Returns a list of all {@link RecentJobEvent} objects stored in the
	 * archive.
	 * 
	 * @return a list of all {@link RecentJobEvent} objects stored in the
	 *         archive
	 */
	List<RecentJobEvent> getJobs();

	/**
	 * Returns the {@link RecentJobEvent} for the given job ID.
	 * 
	 * @param jobId
	 *            the ID of the job to retrieve the {@link RecentJobEvent} for
	 * @return the {@link RecentJobEvent} for the given job ID or
	 *         <code>null</code> if no such event is stored in the archive
	 */
	RecentJobEvent getJob(JobID jobId);

	/**
	 * Returns the archived {@link ManagementGraph} for the job with given ID.
	 * 
	 * @param jobID
	 *            the ID of the job to retrieve the {@link ManagementGraph} for
	 * @return the {@link ManagementGraph} for the given job ID or
	 *         <code>null</code> if no such graph is stored in the archive
	 */
	ManagementGraph getManagementGraph(JobID jobID);

	/**
	 * Returns a list containing all archived {@link AbstractEvent} objects for
	 * the job with the given ID.
	 * 
	 * @param jobID
	 *            the ID of the job to retrieve the archived
	 *            {@link AbstractEvent} objects for
	 * @return a list of all archived {@link AbstractEvent} objects for the job
	 *         with the given ID or
	 *         <code>null<code> if no such events are stored in the archive
	 */
	List<AbstractEvent> getEvents(JobID jobID);

	/**
	 * Returns the point in time when the job with the given ID transitioned
	 * into the specified job status.
	 * 
	 * @param jobID
	 *            the ID of the job to retrieve the time of the specified job
	 *            status transition for
	 * @param jobStatus
	 *            the job status for which the point in time is requested
	 * @return the point in time when the specified job status transition
	 *         occurred or <code>0</code> when the point in time could not be
	 *         retrieved
	 */
	long getJobTime(JobID jobID, JobStatus jobStatus);

	/**
	 * Returns the point in time when the vertex with the given ID transitioned
	 * into the specified execution state.
	 * 
	 * @param jobID
	 *            the ID of the job the vertex ID belongs to
	 * @param jobVertexID
	 *            the ID of the vertex to retrieve the time of the specified
	 *            execute state transition for
	 * @param executionState
	 *            the execution state for which the point in time is requested
	 * @return the point in time when the specified execution state transition
	 *         occurred or <code>0</code> when the point in time could not be
	 *         retrieved
	 */
	long getVertexTime(JobID jobID, ManagementVertexID jobVertexID,
			ExecutionState executionState);
}

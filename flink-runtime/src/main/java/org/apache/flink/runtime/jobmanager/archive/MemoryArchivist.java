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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.ExecutionStateChangeEvent;
import org.apache.flink.runtime.event.job.JobEvent;
import org.apache.flink.runtime.event.job.RecentJobEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.managementgraph.ManagementGraph;
import org.apache.flink.runtime.managementgraph.ManagementVertexID;
import org.apache.flink.runtime.topology.NetworkTopology;

/**
 * Implementation of the ArchiveListener, that archives old data of the jobmanager in memory
 *
 */
public class MemoryArchivist implements ArchiveListener {
	
	
	private int max_entries;
	/**
	 * The map which stores all collected events until they are either
	 * fetched by the client or discarded.
	 */
	private final Map<JobID, List<AbstractEvent>> collectedEvents = new HashMap<JobID, List<AbstractEvent>>();
	
	/**
	 * Map of recently started jobs with the time stamp of the last received job event.
	 */
	private final Map<JobID, RecentJobEvent> oldJobs = new HashMap<JobID, RecentJobEvent>();
	
	/**
	 * Map of management graphs belonging to recently started jobs with the time stamp of the last received job event.
	 */
	private final Map<JobID, ManagementGraph> managementGraphs = new HashMap<JobID, ManagementGraph>();

	/**
	 * Map of network topologies belonging to recently started jobs with the time stamp of the last received job event.
	 */
	private final Map<JobID, NetworkTopology> networkTopologies = new HashMap<JobID, NetworkTopology>();
	
	private final LinkedList<JobID> lru = new LinkedList<JobID>();
	
	public MemoryArchivist(int max_entries) {
		this.max_entries = max_entries;
	}
	
	
	public void archiveEvent(JobID jobId, AbstractEvent event) {
		
		if(!collectedEvents.containsKey(jobId)) {
			collectedEvents.put(jobId, new ArrayList<AbstractEvent>());
		}
		
		collectedEvents.get(jobId).add(event);
		
		cleanup(jobId);
	}
	
	public void archiveJobevent(JobID jobId, RecentJobEvent event) {
		
		oldJobs.put(jobId, event);
		
		cleanup(jobId);
	}
	
	public void archiveManagementGraph(JobID jobId, ManagementGraph graph) {
		
		managementGraphs.put(jobId, graph);
		
		cleanup(jobId);
	}
	
	public void archiveNetworkTopology(JobID jobId, NetworkTopology topology) {
		
		networkTopologies.put(jobId, topology);
		
		cleanup(jobId);
	}

	public List<RecentJobEvent> getJobs() {

		return new ArrayList<RecentJobEvent>(oldJobs.values());
	}
	
	private void cleanup(JobID jobId) {
		if(!lru.contains(jobId)) {
			lru.addFirst(jobId);
		}
		if(lru.size() > this.max_entries) {
			JobID toRemove = lru.removeLast();
			collectedEvents.remove(toRemove);
			oldJobs.remove(toRemove);
			managementGraphs.remove(toRemove);
			networkTopologies.remove(toRemove);
		}
	}
	
	public RecentJobEvent getJob(JobID jobId) {

		return oldJobs.get(jobId);
	}
	
	public ManagementGraph getManagementGraph(final JobID jobID) {

		synchronized (this.managementGraphs) {
			return this.managementGraphs.get(jobID);
		}
	}
	
	public List<AbstractEvent> getEvents(JobID jobID) {
		return collectedEvents.get(jobID);
	}
	
	public long getJobTime(JobID jobID, JobStatus jobStatus) {
		for(AbstractEvent event : this.getEvents(jobID)) {
			if(event instanceof JobEvent)
			{
				if(((JobEvent) event).getCurrentJobStatus() == jobStatus) {
					return event.getTimestamp();
				}
			}
		}
		return 0;
	}
	
	public long getVertexTime(JobID jobID, ManagementVertexID jobVertexID, ExecutionState executionState) {
		for(AbstractEvent event : this.getEvents(jobID)) {
			if(event instanceof ExecutionStateChangeEvent)
			{
				if(((ExecutionStateChangeEvent) event).getVertexID().equals(jobVertexID) && ((ExecutionStateChangeEvent) event).getNewExecutionState().equals(executionState)) {
					return event.getTimestamp();
				}
			}
		}
		return 0;
	}


}

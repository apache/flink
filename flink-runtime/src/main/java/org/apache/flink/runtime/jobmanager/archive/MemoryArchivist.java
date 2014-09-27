/*
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
import org.apache.flink.runtime.event.job.RecentJobEvent;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * Implementation of the ArchiveListener, that archives old data of the JobManager in memory.
 * 
 * This class must be thread safe, because it is accessed by the JobManager events and by the
 * web server concurrently.
 */
public class MemoryArchivist implements ArchiveListener {
	
	/** The global lock */
	private final Object lock = new Object();
	
	/**
	 * The map which stores all collected events until they are either
	 * fetched by the client or discarded.
	 */
	private final Map<JobID, List<AbstractEvent>> collectedEvents = new HashMap<JobID, List<AbstractEvent>>();
	
	/** Map of recently started jobs with the time stamp of the last received job event. */
	private final Map<JobID, RecentJobEvent> oldJobs = new HashMap<JobID, RecentJobEvent>();
	
	/** Map of management graphs belonging to recently started jobs with the time stamp of the last received job event. */
	private final Map<JobID, ExecutionGraph> graphs = new HashMap<JobID, ExecutionGraph>();
	
	private final LinkedList<JobID> lru = new LinkedList<JobID>();
	
	private final int max_entries;
	
	// --------------------------------------------------------------------------------------------
	
	public MemoryArchivist(int max_entries) {
		this.max_entries = max_entries;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void archiveExecutionGraph(JobID jobId, ExecutionGraph graph) {
		synchronized (lock) {
			graphs.put(jobId, graph);
			cleanup(jobId);
		}
	}
	
	@Override
	public void archiveEvent(JobID jobId, AbstractEvent event) {
		synchronized (lock) {
			if(!collectedEvents.containsKey(jobId)) {
				collectedEvents.put(jobId, new ArrayList<AbstractEvent>());
			}
			
			collectedEvents.get(jobId).add(event);
			cleanup(jobId);
		}
	}
	
	@Override
	public void archiveJobevent(JobID jobId, RecentJobEvent event) {
		synchronized (lock) {
			oldJobs.put(jobId, event);
			cleanup(jobId);
		}
	}

	@Override
	public List<RecentJobEvent> getJobs() {
		synchronized (lock) {
			return new ArrayList<RecentJobEvent>(oldJobs.values());
		}
	}
	
	@Override
	public RecentJobEvent getJob(JobID jobId) {
		synchronized (lock) {
			return oldJobs.get(jobId);
		}
	}
	
	@Override
	public List<AbstractEvent> getEvents(JobID jobID) {
		synchronized (graphs) {
			return collectedEvents.get(jobID);
		}
	}

	@Override
	public ExecutionGraph getExecutionGraph(JobID jid) {
		synchronized (lock) {
			return graphs.get(jid);
		}
	}
	
	
	
	private void cleanup(JobID jobId) {
		if (!lru.contains(jobId)) {
			lru.addFirst(jobId);
		}
		if (lru.size() > this.max_entries) {
			JobID toRemove = lru.removeLast();
			collectedEvents.remove(toRemove);
			oldJobs.remove(toRemove);
			graphs.remove(toRemove);
		}
	}
}

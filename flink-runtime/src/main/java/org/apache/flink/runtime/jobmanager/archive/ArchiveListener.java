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

import java.util.List;

import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.RecentJobEvent;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * Interface used to implement Archivists, that store old JobManager information discarded by the EventCollector.
 * Archivists can decide how to store the data (memory, database, files...)
 */
public interface ArchiveListener {
	
	
	void archiveExecutionGraph(JobID jobId, ExecutionGraph graph);
	
	/**
	 * Stores event in archive
	 */
	void archiveEvent(JobID jobId, AbstractEvent event);
	
	/**
	 * Stores old job in archive
	 */
	void archiveJobevent(JobID jobId, RecentJobEvent event);
	
	/**
	 * Get all archived Jobs
	 */
	List<RecentJobEvent> getJobs();
	
	/**
	 * Return archived job
	 */
	RecentJobEvent getJob(JobID JobId);
	
	/**
	 * Get all archived Events for a job
	 */
	List<AbstractEvent> getEvents(JobID jobID);	
	
	ExecutionGraph getExecutionGraph(JobID jid);
}

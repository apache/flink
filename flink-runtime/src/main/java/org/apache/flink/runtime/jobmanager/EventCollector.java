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

package org.apache.flink.runtime.jobmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.ExecutionStateChangeEvent;
import org.apache.flink.runtime.event.job.JobEvent;
import org.apache.flink.runtime.event.job.ManagementEvent;
import org.apache.flink.runtime.event.job.RecentJobEvent;
import org.apache.flink.runtime.event.job.VertexEvent;
import org.apache.flink.runtime.execution.ExecutionListener;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.archive.ArchiveListener;
import org.apache.flink.runtime.profiling.ProfilingListener;
import org.apache.flink.runtime.profiling.types.ProfilingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The event collector collects events which occurred during the execution of a job and prepares them
 * for being fetched by a client. The collected events have an expiration time. In a configurable interval
 * the event collector removes all intervals which are older than the interval.
 */
public final class EventCollector extends TimerTask implements ProfilingListener {

	private static final Logger LOG = LoggerFactory.getLogger(EventCollector.class);

	/**
	 * The execution listener wrapper is an auxiliary class. It is required
	 * because the job vertex ID and the management vertex ID cannot be accessed from
	 * the data provided by the <code>executionStateChanged</code> callback method.
	 * However, these IDs are needed to create the construct the {@link VertexEvent} and the
	 * {@link ExecutionStateChangeEvent}.
	 */
	private static final class ExecutionListenerWrapper implements ExecutionListener {

		/** The event collector to forward the created event to. */
		private final EventCollector eventCollector;

		private final ExecutionGraph graph;
		

		public ExecutionListenerWrapper(EventCollector eventCollector, ExecutionGraph graph) {
			this.eventCollector = eventCollector;
			this.graph = graph;
		}

		@Override
		public void executionStateChanged(JobID jobID, JobVertexID vertexId, int subtask, ExecutionAttemptID executionId,
				ExecutionState newExecutionState, String optionalMessage)
		{
			final long timestamp = System.currentTimeMillis();

			final ExecutionJobVertex vertex = graph.getJobVertex(vertexId);
			
			final String taskName = vertex == null ? "(null)" : vertex.getJobVertex().getName();
			final int totalNumberOfSubtasks = vertex == null ? -1 : vertex.getParallelism();

			// Create a new vertex event
			final VertexEvent vertexEvent = new VertexEvent(timestamp, vertexId, taskName, totalNumberOfSubtasks,
					subtask, executionId, newExecutionState, optionalMessage);

			this.eventCollector.addEvent(jobID, vertexEvent);

			final ExecutionStateChangeEvent executionStateChangeEvent = new ExecutionStateChangeEvent(timestamp, vertexId, subtask,
					executionId, newExecutionState);

			this.eventCollector.addEvent(jobID, executionStateChangeEvent);
			
			LOG.info(vertexEvent.toString());
		}
	}

	/**
	 * The job status listener wrapper is an auxiliary class. It is required
	 * because the job name cannot be accessed from the data provided by the <code>jobStatusHasChanged</code> callback
	 * method. However, this job name
	 * is needed to create the construct the {@link RecentJobEvent}.
	 * 
	 */
	private static final class JobStatusListenerWrapper implements JobStatusListener {

		/** The event collector to forward the created event to. */
		private final EventCollector eventCollector;

		/** The name of the job this wrapper has been created for. */
		private final String jobName;

		/** <code>true</code> if profiling events are collected for the job, <code>false</code> otherwise. */
		private final boolean isProfilingAvailable;

		/** The time stamp of the job submission */
		private final long submissionTimestamp;

		/**
		 * Constructs a new job status listener wrapper.
		 * 
		 * @param eventCollector
		 *        the event collector to forward the events to
		 * @param jobName
		 *        the name of the job
		 * @param isProfilingAvailable
		 *        <code>true</code> if profiling events are collected for the job, <code>false</code> otherwise
		 * @param submissionTimestamp
		 *        the submission time stamp of the job
		 */
		public JobStatusListenerWrapper(EventCollector eventCollector, String jobName,
				boolean isProfilingAvailable, long submissionTimestamp)
		{
			this.eventCollector = eventCollector;
			this.jobName = jobName;
			this.isProfilingAvailable = isProfilingAvailable;
			this.submissionTimestamp = submissionTimestamp;
		}

		@Override
		public void jobStatusHasChanged(ExecutionGraph executionGraph, JobStatus newJobStatus, String optionalMessage) {

			final JobID jobID = executionGraph.getJobID();

			if (newJobStatus == JobStatus.RUNNING) {
				this.eventCollector.addExecutionGraph(jobID, executionGraph);
			}

			// Update recent job event
			this.eventCollector.updateRecentJobEvent(jobID, this.jobName, this.isProfilingAvailable,
					this.submissionTimestamp, newJobStatus);

			this.eventCollector.addEvent(jobID,
					new JobEvent(System.currentTimeMillis(), newJobStatus, optionalMessage));
		}
	}

	private final long timerTaskInterval;

	/**
	 * The map which stores all collected events until they are either
	 * fetched by the client or discarded.
	 */
	private final Map<JobID, List<AbstractEvent>> collectedEvents = new HashMap<JobID, List<AbstractEvent>>();

	/**
	 * Map of recently started jobs with the time stamp of the last received job event.
	 */
	private final Map<JobID, RecentJobEvent> recentJobs = new HashMap<JobID, RecentJobEvent>();

	/**
	 * Map of management graphs belonging to recently started jobs with the time stamp of the last received job event.
	 */
	private final Map<JobID, ExecutionGraph> recentManagementGraphs = new HashMap<JobID, ExecutionGraph>();

	/**
	 * The timer used to trigger the cleanup routine.
	 */
	private final Timer timer;
	
	private List<ArchiveListener> archivists = new ArrayList<ArchiveListener>();

	/**
	 * Constructs a new event collector and starts
	 * its background cleanup routine.
	 * 
	 * @param clientQueryInterval
	 *        the interval with which clients query for events
	 */
	public EventCollector(final int clientQueryInterval) {

		this.timerTaskInterval = clientQueryInterval * 1000L * 2L; // Double the interval, clients will take care of
		// duplicate notifications

		this.timer = new Timer();
		this.timer.schedule(this, this.timerTaskInterval, this.timerTaskInterval);
	}

	/**
	 * Retrieves and adds the collected events for the job with the given job ID to the provided list.
	 * 
	 * @param jobID
	 *        the ID of the job to retrieve the events for
	 * @param eventList
	 *        the list to which the events shall be added
	 * @param includeManagementEvents
	 *        <code>true</code> if {@link ManagementEvent} objects shall be added to the list as well,
	 *        <code>false</code> otherwise
	 */
	public void getEventsForJob(JobID jobID, List<AbstractEvent> eventList, boolean includeManagementEvents) {

		synchronized (this.collectedEvents) {

			List<AbstractEvent> eventsForJob = this.collectedEvents.get(jobID);
			if (eventsForJob != null) {

				final Iterator<AbstractEvent> it = eventsForJob.iterator();
				while (it.hasNext()) {

					final AbstractEvent event = it.next();
					final boolean isManagementEvent = (event instanceof ManagementEvent);
					if (!isManagementEvent || includeManagementEvents) {
						eventList.add(event);
					}
				}
			}
		}
	}

	public void getRecentJobs(List<RecentJobEvent> eventList) {
		synchronized (this.recentJobs) {
			eventList.addAll(this.recentJobs.values());
		}
	}

	/**
	 * Stops the timer thread and cleans up the
	 * data structure which stores the collected events.
	 */
	public void shutdown() {

		// Clear event map
		synchronized (this.collectedEvents) {
			this.collectedEvents.clear();
		}

		synchronized (this.recentJobs) {
			this.recentJobs.clear();
		}

		// Cancel the timer for the cleanup routine
		this.timer.cancel();
	}

	/**
	 * Adds an event to the job's event list.
	 * 
	 * @param jobID
	 *        the ID of the job the event belongs to
	 * @param event
	 *        the event to be added to the job's event list
	 */
	private void addEvent(JobID jobID, AbstractEvent event) {

		synchronized (this.collectedEvents) {

			List<AbstractEvent> eventList = this.collectedEvents.get(jobID);
			if (eventList == null) {
				eventList = new ArrayList<AbstractEvent>();
				this.collectedEvents.put(jobID, eventList);
			}

			eventList.add(event);
		}
	}

	/**
	 * Creates a {@link RecentJobEvent} and adds it to the list of recent jobs.
	 * 
	 * @param jobID
	 *        the ID of the new job
	 * @param jobName
	 *        the name of the new job
	 * @param isProfilingEnabled
	 *        <code>true</code> if profiling events are collected for the job, <code>false</code> otherwise
	 * @param submissionTimestamp
	 *        the submission time stamp of the job
	 * @param jobStatus
	 *        the status of the job
	 */
	private void updateRecentJobEvent(JobID jobID, String jobName, boolean isProfilingEnabled,
			long submissionTimestamp, JobStatus jobStatus)
	{
		final long currentTime = System.currentTimeMillis();
		
		final RecentJobEvent recentJobEvent = new RecentJobEvent(jobID, jobName, jobStatus, isProfilingEnabled,
			submissionTimestamp, currentTime);

		synchronized (this.recentJobs) {
			this.recentJobs.put(jobID, recentJobEvent);
		}
	}

	/**
	 * Registers a job in form of its execution graph representation
	 * with the job progress collector. The collector will subscribe
	 * to state changes of the individual subtasks. A separate
	 * de-registration is not necessary since the job progress collector
	 * periodically discards outdated progress information.
	 * 
	 * @param executionGraph
	 *        the execution graph representing the job
	 * @param profilingAvailable
	 *        indicates if profiling data is available for this job
	 * @param submissionTimestamp
	 *        the submission time stamp of the job
	 */
	public void registerJob(ExecutionGraph executionGraph, boolean profilingAvailable, long submissionTimestamp) {

		executionGraph.registerExecutionListener(new ExecutionListenerWrapper(this, executionGraph));

		executionGraph.registerJobStatusListener(new JobStatusListenerWrapper(this, executionGraph.getJobName(),
			profilingAvailable, submissionTimestamp));
	}

	/**
	 * This method will periodically be called to clean up expired
	 * collected events.
	 */
	@Override
	public void run() {

		final long currentTime = System.currentTimeMillis();

		synchronized (this.collectedEvents) {

			final Iterator<JobID> it = this.collectedEvents.keySet().iterator();
			while (it.hasNext()) {

				final JobID jobID = it.next();
				final List<AbstractEvent> eventList = this.collectedEvents.get(jobID);
				if (eventList == null) {
					continue;
				}

				final Iterator<AbstractEvent> it2 = eventList.iterator();
				while (it2.hasNext()) {

					final AbstractEvent event = it2.next();
					// If the event is older than TIMERTASKINTERVAL, remove it
					if ((event.getTimestamp() + this.timerTaskInterval) < currentTime) {
						archiveEvent(jobID, event);
						it2.remove();
					}
				}

				if (eventList.isEmpty()) {
					it.remove();
				}
			}
		}

		synchronized (this.recentJobs) {

			final Iterator<Map.Entry<JobID, RecentJobEvent>> it = this.recentJobs.entrySet().iterator();
			while (it.hasNext()) {

				final Map.Entry<JobID, RecentJobEvent> entry = it.next();
				final JobStatus jobStatus = entry.getValue().getJobStatus();

				// Only remove jobs from the list which have stopped running
				if (jobStatus != JobStatus.FINISHED && jobStatus != JobStatus.CANCELED
					&& jobStatus != JobStatus.FAILED) {
					continue;
				}

				// Check time stamp of last job status update
				if ((entry.getValue().getTimestamp() + this.timerTaskInterval) < currentTime) {
					archiveJobevent(entry.getKey(), entry.getValue());
					it.remove();
					synchronized (this.recentManagementGraphs) {
						archiveManagementGraph(entry.getKey(), this.recentManagementGraphs.get(entry.getKey()));
						this.recentManagementGraphs.remove(entry.getValue());
					}
				}
			}
		}
	}


	@Override
	public void processProfilingEvents(final ProfilingEvent profilingEvent) {
		// Simply add profiling events to the job's event queue
		addEvent(profilingEvent.getJobID(), profilingEvent);
	}

	/**
	 * Adds an execution graph to the map of recently created management graphs.
	 * 
	 * @param jobID The ID of the graph
	 * @param executionGraph The graph to be added
	 */
	void addExecutionGraph(JobID jobID, ExecutionGraph executionGraph) {
		synchronized (this.recentManagementGraphs) {
			this.recentManagementGraphs.put(jobID, executionGraph);
		}
	}

	/**
	 * Returns the execution graph object for the job with the given ID from the map of recently added graphs.
	 * 
	 * @param jobID The ID of the job the management graph shall be retrieved for
	 * @return the management graph for the job with the given ID or <code>null</code> if no such graph exists
	 */
	public ExecutionGraph getManagementGraph(JobID jobID) {
		synchronized (this.recentManagementGraphs) {
			return this.recentManagementGraphs.get(jobID);
		}
	}
	
	/**
	 * Register Archivist to archive 
	 */
	public void registerArchivist(ArchiveListener al) {
		this.archivists.add(al);
	}
	
	private void archiveEvent(JobID jobId, AbstractEvent event) {
		for (ArchiveListener al : archivists) {
			al.archiveEvent(jobId, event);
		}
	}
	
	private void archiveJobevent(JobID jobId, RecentJobEvent event) {
		for (ArchiveListener al : archivists) {
			al.archiveJobevent(jobId, event);
		}
	}
	
	private void archiveManagementGraph(JobID jobId, ExecutionGraph graph) {
		for (ArchiveListener al : archivists) {
			al.archiveExecutionGraph(jobId, graph);
		}
	}
}

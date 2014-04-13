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

package eu.stratosphere.nephele.jobmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.ExecutionStateChangeEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.event.job.ManagementEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.event.job.VertexAssignmentEvent;
import eu.stratosphere.nephele.event.job.VertexEvent;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.executiongraph.JobStatusListener;
import eu.stratosphere.nephele.executiongraph.ManagementGraphFactory;
import eu.stratosphere.nephele.executiongraph.VertexAssignmentListener;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.jobmanager.archive.ArchiveListener;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.profiling.ProfilingListener;
import eu.stratosphere.nephele.profiling.types.ProfilingEvent;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * The event collector collects events which occurred during the execution of a job and prepares them
 * for being fetched by a client. The collected events have an expiration time. In a configurable interval
 * the event collector removes all intervals which are older than the interval.
 * <p>
 * This class is thread-safe.
 * 
 */
public final class EventCollector extends TimerTask implements ProfilingListener {

	/**
	 * The execution listener wrapper is an auxiliary class. It is required
	 * because the job vertex ID and the management vertex ID cannot be accessed from
	 * the data provided by the <code>executionStateChanged</code> callback method.
	 * However, these IDs are needed to create the construct the {@link VertexEvent} and the
	 * {@link ExecutionStateChangeEvent}.
	 * 
	 */
	private static final class ExecutionListenerWrapper implements ExecutionListener {

		/**
		 * The event collector to forward the created event to.
		 */
		private final EventCollector eventCollector;

		/**
		 * The vertex this listener belongs to.
		 */
		private final ExecutionVertex vertex;

		/**
		 * Constructs a new execution listener object.
		 * 
		 * @param eventCollector
		 *        the event collector to forward the created event to
		 * @param vertex
		 *        the vertex this listener belongs to.
		 */
		public ExecutionListenerWrapper(final EventCollector eventCollector, final ExecutionVertex vertex) {
			this.eventCollector = eventCollector;
			this.vertex = vertex;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void executionStateChanged(final JobID jobID, final ExecutionVertexID vertexID,
				final ExecutionState newExecutionState, final String optionalMessage) {

			final long timestamp = System.currentTimeMillis();

			final JobVertexID jobVertexID = this.vertex.getGroupVertex().getJobVertexID();
			final String taskName = this.vertex.getGroupVertex().getName();
			final int totalNumberOfSubtasks = this.vertex.getGroupVertex().getCurrentNumberOfGroupMembers();
			final int indexInSubtaskGroup = this.vertex.getIndexInVertexGroup();

			// Create a new vertex event
			final VertexEvent vertexEvent = new VertexEvent(timestamp, jobVertexID, taskName, totalNumberOfSubtasks,
				indexInSubtaskGroup, newExecutionState, optionalMessage);

			this.eventCollector.addEvent(jobID, vertexEvent);

			final ExecutionStateChangeEvent executionStateChangeEvent = new ExecutionStateChangeEvent(timestamp,
				vertexID.toManagementVertexID(), newExecutionState);

			this.eventCollector.updateManagementGraph(jobID, executionStateChangeEvent, optionalMessage);
			this.eventCollector.addEvent(jobID, executionStateChangeEvent);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void userThreadStarted(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
			// Nothing to do here
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void userThreadFinished(final JobID jobID, final ExecutionVertexID vertexID, final Thread userThread) {
			// Nothing to do here
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int getPriority() {

			return 20;
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

		/**
		 * The event collector to forward the created event to.
		 */
		private final EventCollector eventCollector;

		/**
		 * The name of the job this wrapper has been created for.
		 */
		private final String jobName;

		/**
		 * <code>true</code> if profiling events are collected for the job, <code>false</code> otherwise.
		 */
		private final boolean isProfilingAvailable;

		/**
		 * The time stamp of the job submission
		 */
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
		public JobStatusListenerWrapper(final EventCollector eventCollector, final String jobName,
				final boolean isProfilingAvailable, final long submissionTimestamp) {

			this.eventCollector = eventCollector;
			this.jobName = jobName;
			this.isProfilingAvailable = isProfilingAvailable;
			this.submissionTimestamp = submissionTimestamp;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void jobStatusHasChanged(final ExecutionGraph executionGraph, final InternalJobStatus newJobStatus,
				final String optionalMessage) {

			final JobID jobID = executionGraph.getJobID();

			if (newJobStatus == InternalJobStatus.SCHEDULED) {

				final ManagementGraph managementGraph = ManagementGraphFactory.fromExecutionGraph(executionGraph);
				this.eventCollector.addManagementGraph(jobID, managementGraph);
			}

			// Update recent job event
			final JobStatus jobStatus = InternalJobStatus.toJobStatus(newJobStatus);
			if (jobStatus != null) {
				this.eventCollector.updateRecentJobEvent(jobID, this.jobName, this.isProfilingAvailable,
					this.submissionTimestamp, jobStatus);

				this.eventCollector.addEvent(jobID,
					new JobEvent(System.currentTimeMillis(), jobStatus, optionalMessage));
			}
		}
	}

	/**
	 * The vertex assignment listener wrapper is an auxiliary class. It is required
	 * because the job ID cannot be accessed from the data provided by the <code>vertexAssignmentChanged</code> callback
	 * method. However, this job ID is needed to prepare the {@link VertexAssignmentEvent} for transmission.
	 * 
	 */
	private static final class VertexAssignmentListenerWrapper implements VertexAssignmentListener {

		/**
		 * The event collector to forward the created event to.
		 */
		private final EventCollector eventCollector;

		/**
		 * The ID the job this wrapper has been created for.
		 */
		private final JobID jobID;

		/**
		 * Constructs a new vertex assignment listener wrapper.
		 * 
		 * @param eventCollector
		 *        the event collector to forward the events to
		 * @param jobID
		 *        the ID of the job
		 */
		public VertexAssignmentListenerWrapper(final EventCollector eventCollector, final JobID jobID) {
			this.eventCollector = eventCollector;
			this.jobID = jobID;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void vertexAssignmentChanged(final ExecutionVertexID id, final AllocatedResource newAllocatedResource) {

			// Create a new vertex assignment event
			final ManagementVertexID managementVertexID = id.toManagementVertexID();
			final long timestamp = System.currentTimeMillis();

			final AbstractInstance instance = newAllocatedResource.getInstance();
			VertexAssignmentEvent event;
			if (instance == null) {
				event = new VertexAssignmentEvent(timestamp, managementVertexID, "null", "null");
			} else {

				String instanceName = null;
				if (instance.getInstanceConnectionInfo() != null) {
					instanceName = instance.getInstanceConnectionInfo().toString();
				} else {
					instanceName = instance.toString();
				}

				event = new VertexAssignmentEvent(timestamp, managementVertexID, instanceName, instance.getType()
					.getIdentifier());
			}

			this.eventCollector.updateManagementGraph(jobID, event);
			this.eventCollector.addEvent(this.jobID, event);
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
	private final Map<JobID, ManagementGraph> recentManagementGraphs = new HashMap<JobID, ManagementGraph>();

	/**
	 * Map of network topologies belonging to recently started jobs with the time stamp of the last received job event.
	 */
	private final Map<JobID, NetworkTopology> recentNetworkTopologies = new HashMap<JobID, NetworkTopology>();

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
	public void getEventsForJob(final JobID jobID, final List<AbstractEvent> eventList,
			final boolean includeManagementEvents) {

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

			final Iterator<RecentJobEvent> it = this.recentJobs.values().iterator();
			while (it.hasNext()) {
				eventList.add(it.next());
			}
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
	private void updateRecentJobEvent(final JobID jobID, final String jobName, final boolean isProfilingEnabled,
			final long submissionTimestamp, final JobStatus jobStatus) {

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
	 * deregistration is not necessary since the job progress collector
	 * periodically discards outdated progress information.
	 * 
	 * @param executionGraph
	 *        the execution graph representing the job
	 * @param profilingAvailable
	 *        indicates if profiling data is available for this job
	 * @param submissionTimestamp
	 *        the submission time stamp of the job
	 */
	public void registerJob(final ExecutionGraph executionGraph, final boolean profilingAvailable,
			final long submissionTimestamp) {

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(executionGraph, true);

		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();

			// Register the listener object which will pass state changes on to the collector
			vertex.registerExecutionListener(new ExecutionListenerWrapper(this, vertex));

			// Register the listener object which will pass assignment changes on to the collector
			vertex.registerVertexAssignmentListener(new VertexAssignmentListenerWrapper(this, executionGraph.getJobID()));
		}

		// Register one job status listener wrapper for the entire job
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
					synchronized (this.recentNetworkTopologies) {
						archiveNetworkTopology(entry.getKey(), this.recentNetworkTopologies.get(entry.getKey()));
						this.recentNetworkTopologies.remove(entry.getValue());
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
	 * Adds a {@link ManagementGraph} to the map of recently created management graphs.
	 * 
	 * @param jobID
	 *        the ID of the job the management graph belongs to
	 * @param managementGraph
	 *        the management graph to be added
	 */
	void addManagementGraph(final JobID jobID, final ManagementGraph managementGraph) {

		synchronized (this.recentManagementGraphs) {
			this.recentManagementGraphs.put(jobID, managementGraph);
		}
	}

	/**
	 * Returns the {@link ManagementGraph} object for the job with the given ID from the map of recently created
	 * management graphs.
	 * 
	 * @param jobID
	 *        the ID of the job the management graph shall be retrieved for
	 * @return the management graph for the job with the given ID or <code>null</code> if no such graph exists
	 */
	public ManagementGraph getManagementGraph(final JobID jobID) {

		synchronized (this.recentManagementGraphs) {
			return this.recentManagementGraphs.get(jobID);
		}
	}

	/**
	 * Applies changes in the vertex assignment to the stored management graph.
	 * 
	 * @param jobID
	 *        the ID of the job whose management graph shall be updated
	 * @param vertexAssignmentEvent
	 *        the event describing the changes in the vertex assignment
	 */
	private void updateManagementGraph(final JobID jobID, final VertexAssignmentEvent vertexAssignmentEvent) {

		synchronized (this.recentManagementGraphs) {

			final ManagementGraph managementGraph = this.recentManagementGraphs.get(jobID);
			if (managementGraph == null) {
				return;
			}
			final ManagementVertex vertex = managementGraph.getVertexByID(vertexAssignmentEvent.getVertexID());
			if (vertex == null) {
				return;
			}

			vertex.setInstanceName(vertexAssignmentEvent.getInstanceName());
			vertex.setInstanceType(vertexAssignmentEvent.getInstanceType());
		}
	}

	/**
	 * Applies changes in the state of an execution vertex to the stored management graph.
	 * 
	 * @param jobID
	 *        the ID of the job whose management graph shall be updated
	 * @param executionStateChangeEvent
	 *        the event describing the changes in the execution state of the vertex
	 */
	private void updateManagementGraph(final JobID jobID, final ExecutionStateChangeEvent executionStateChangeEvent, String optionalMessage) {

		synchronized (this.recentManagementGraphs) {

			final ManagementGraph managementGraph = this.recentManagementGraphs.get(jobID);
			if (managementGraph == null) {
				return;
			}
			final ManagementVertex vertex = managementGraph.getVertexByID(executionStateChangeEvent.getVertexID());
			if (vertex == null) {
				return;
			}

			vertex.setExecutionState(executionStateChangeEvent.getNewExecutionState());
			if (executionStateChangeEvent.getNewExecutionState() == ExecutionState.FAILED) {
				vertex.setOptMessage(optionalMessage);
			}
		}
	}
	
	/**
	 * Register Archivist to archive 
	 */
	public void registerArchivist(ArchiveListener al) {
		this.archivists.add(al);
	}
	
	private void archiveEvent(JobID jobId, AbstractEvent event) {
		for(ArchiveListener al : archivists) {
			al.archiveEvent(jobId, event);
		}
	}
	
	private void archiveJobevent(JobID jobId, RecentJobEvent event) {
		for(ArchiveListener al : archivists) {
			al.archiveJobevent(jobId, event);
		}
	}
	
	private void archiveManagementGraph(JobID jobId, ManagementGraph graph) {
		for(ArchiveListener al : archivists) {
			al.archiveManagementGraph(jobId, graph);
		}
	}
	
	private void archiveNetworkTopology(JobID jobId, NetworkTopology topology) {
		for(ArchiveListener al : archivists) {
			al.archiveNetworkTopology(jobId, topology);
		}
	}
}

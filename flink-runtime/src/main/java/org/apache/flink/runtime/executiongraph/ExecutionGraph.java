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

package org.apache.flink.runtime.executiongraph;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.execution.ExecutionListener;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.io.network.ConnectionInfoLookupResponse;
import org.apache.flink.runtime.io.network.RemoteReceiver;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.ExceptionUtils;


public class ExecutionGraph {

	private static final AtomicReferenceFieldUpdater<ExecutionGraph, JobStatus> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(ExecutionGraph.class, JobStatus.class, "state");
	
	/** The log object used for debugging. */
	static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

	// --------------------------------------------------------------------------------------------
	
	/** The ID of the job this graph has been built for. */
	private final JobID jobID;

	/** The name of the original job graph. */
	private final String jobName;

	/** The job configuration that was originally attached to the JobGraph. */
	private final Configuration jobConfiguration;
	
	/** All job vertices that are part of this graph */
	private final ConcurrentHashMap<JobVertexID, ExecutionJobVertex> tasks;
	
	/** All vertices, in the order in which they were created **/
	private final List<ExecutionJobVertex> verticesInCreationOrder;

	/** All intermediate results that are part of this graph */
	private final ConcurrentHashMap<IntermediateDataSetID, IntermediateResult> intermediateResults;
	
	/** The currently executed tasks, for callbacks */
	private final ConcurrentHashMap<ExecutionAttemptID, Execution> currentExecutions;

	
	
	private final Map<ChannelID, ExecutionEdge> edges = new HashMap<ChannelID, ExecutionEdge>();
	
	
	/** An executor that can run long actions (involving remote calls) */
	private final ExecutorService executor;

	private final List<BlobKey> requiredJarFiles;
	
	private final List<JobStatusListener> jobStatusListeners;
	
	private final List<ExecutionListener> executionListeners;
	
	private final long[] stateTimestamps;
	
	
	private final Object progressLock = new Object();
	
	private int nextVertexToFinish;
	
	private volatile JobStatus state = JobStatus.CREATED;
	
	private volatile Throwable failureCause;
	
	
	private Scheduler scheduler;
	
	private boolean allowQueuedScheduling = true;
	
	
	public ExecutionGraph(JobID jobId, String jobName, Configuration jobConfig) {
		this(jobId, jobName, jobConfig, new ArrayList<BlobKey>(), null);
	}
	
	public ExecutionGraph(JobID jobId, String jobName, Configuration jobConfig,
						List<BlobKey> requiredJarFiles,ExecutorService executor) {
		if (jobId == null || jobName == null || jobConfig == null) {
			throw new NullPointerException();
		}
		
		this.jobID = jobId;
		this.jobName = jobName;
		this.jobConfiguration = jobConfig;
		this.executor = executor;
		
		this.tasks = new ConcurrentHashMap<JobVertexID, ExecutionJobVertex>();
		this.intermediateResults = new ConcurrentHashMap<IntermediateDataSetID, IntermediateResult>();
		this.verticesInCreationOrder = new ArrayList<ExecutionJobVertex>();
		this.currentExecutions = new ConcurrentHashMap<ExecutionAttemptID, Execution>();
		
		this.jobStatusListeners = new CopyOnWriteArrayList<JobStatusListener>();
		this.executionListeners = new CopyOnWriteArrayList<ExecutionListener>();
		
		this.stateTimestamps = new long[JobStatus.values().length];
		this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

		this.requiredJarFiles = requiredJarFiles;
	}

	// --------------------------------------------------------------------------------------------
	
	public void attachJobGraph(List<AbstractJobVertex> topologiallySorted) throws JobException {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Attaching %d topologically sorted vertices to existing job graph with %d "
					+ "vertices and %d intermediate results.", topologiallySorted.size(), tasks.size(), intermediateResults.size()));
		}
		
		final long createTimestamp = System.currentTimeMillis();
		
		for (AbstractJobVertex jobVertex : topologiallySorted) {
			
			// create the execution job vertex and attach it to the graph
			ExecutionJobVertex ejv = new ExecutionJobVertex(this, jobVertex, 1, createTimestamp);
			ejv.connectToPredecessors(this.intermediateResults);
			
			ExecutionJobVertex previousTask = this.tasks.putIfAbsent(jobVertex.getID(), ejv);
			if (previousTask != null) {
				throw new JobException(String.format("Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
						jobVertex.getID(), ejv, previousTask));
			}
			
			for (IntermediateResult res : ejv.getProducedDataSets()) {
				IntermediateResult previousDataSet = this.intermediateResults.putIfAbsent(res.getId(), res);
				if (previousDataSet != null) {
					throw new JobException(String.format("Encountered two intermediate data set with ID %s : previous=[%s] / new=[%s]",
							res.getId(), res, previousDataSet));
				}
			}
			
			this.verticesInCreationOrder.add(ejv);
		}
	}

	/**
	 * Returns a list of BLOB keys referring to the JAR files required to run this job
	 * @return list of BLOB keys referring to the JAR files required to run this job
	 */
	public List<BlobKey> getRequiredJarFiles() {
		return this.requiredJarFiles;
	}

	// --------------------------------------------------------------------------------------------
	
	public JobID getJobID() {
		return jobID;
	}
	
	public String getJobName() {
		return jobName;
	}
	
	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}
	
	public JobStatus getState() {
		return state;
	}
	
	public ExecutionJobVertex getJobVertex(JobVertexID id) {
		return this.tasks.get(id);
	}
	
	public Map<JobVertexID, ExecutionJobVertex> getAllVertices() {
		return Collections.unmodifiableMap(this.tasks);
	}
	
	public Iterable<ExecutionJobVertex> getVerticesTopologically() {
		// we return a specific iterator that does not fail with concurrent modifications
		// the list is append only, so it is safe for that
		final int numElements = this.verticesInCreationOrder.size();
		
		return new Iterable<ExecutionJobVertex>() {
			@Override
			public Iterator<ExecutionJobVertex> iterator() {
				return new Iterator<ExecutionJobVertex>() {
					private int pos = 0;

					@Override
					public boolean hasNext() {
						return pos < numElements;
					}

					@Override
					public ExecutionJobVertex next() {
						if (hasNext()) {
							return verticesInCreationOrder.get(pos++);
						} else {
							throw new NoSuchElementException();
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}
	
	public Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults() {
		return Collections.unmodifiableMap(this.intermediateResults);
	}
	
	public Iterable<ExecutionVertex> getAllExecutionVertices() {
		return new Iterable<ExecutionVertex>() {
			@Override
			public Iterator<ExecutionVertex> iterator() {
				return new AllVerticesIterator(getVerticesTopologically().iterator());
			}
		};
	}
	
	public long getStatusTimestamp(JobStatus status) {
		return this.stateTimestamps[status.ordinal()];
	}
	
	public boolean isQueuedSchedulingAllowed() {
		return this.allowQueuedScheduling;
	}
	
	public void setQueuedSchedulingAllowed(boolean allowed) {
		this.allowQueuedScheduling = allowed;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------
	
	public void scheduleForExecution(Scheduler scheduler) throws JobException {
		if (scheduler == null) {
			throw new IllegalArgumentException("Scheduler must not be null.");
		}
		
		if (this.scheduler != null && this.scheduler != scheduler) {
			throw new IllegalArgumentException("Cann not use different schedulers for the same job");
		}
		
		if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
			this.scheduler = scheduler;
			
			// initially, we simply take the ones without inputs.
			// next, we implement the logic to go back from vertices that need computation
			// to the ones we need to start running
			for (ExecutionJobVertex ejv : this.tasks.values()) {
				if (ejv.getJobVertex().isInputVertex()) {
					ejv.scheduleAll(scheduler, allowQueuedScheduling);
				}
			}
		}
		else {
			throw new IllegalStateException("Job may only be scheduled from state " + JobStatus.CREATED);
		}
	}
	
	public void cancel() {
		while (true) {
			JobStatus current = state;
			
			if (current == JobStatus.RUNNING || current == JobStatus.CREATED) {
				if (transitionState(current, JobStatus.CANCELLING)) {
					for (ExecutionJobVertex ejv : verticesInCreationOrder) {
						ejv.cancel();
					}
					return;
				}
			}
			else {
				// no need to treat other states
				return;
			}
		}
	}
	
	public void fail(Throwable t) {
		while (true) {
			JobStatus current = state;
			if (current == JobStatus.FAILED || current == JobStatus.FAILING) {
				return;
			}
			else if (transitionState(current, JobStatus.FAILING, t)) {
				this.failureCause = t;
				
				// cancel all. what is failed will not cancel but stay failed
				for (ExecutionJobVertex ejv : verticesInCreationOrder) {
					ejv.cancel();
				}
				
				return;
			}
			
			// no need to treat other states
		}
	}
	
	public void waitForJobEnd(long timeout) throws InterruptedException {
		synchronized (progressLock) {
			while (nextVertexToFinish < verticesInCreationOrder.size()) {
				progressLock.wait(timeout);
			}
		}
	}
	
	public void waitForJobEnd() throws InterruptedException {
		waitForJobEnd(0);
	}
	
	
	private boolean transitionState(JobStatus current, JobStatus newState) {
		return transitionState(current, newState, null);
	}
	
	private boolean transitionState(JobStatus current, JobStatus newState, Throwable error) {
		if (STATE_UPDATER.compareAndSet(this, current, newState)) {
			stateTimestamps[newState.ordinal()] = System.currentTimeMillis();
			notifyJobStatusChange(newState, error);
			return true;
		}
		else {
			return false;
		}
	}
	
	void jobVertexInFinalState(ExecutionJobVertex ev) {
		synchronized (progressLock) {
			int nextPos = nextVertexToFinish;
			if (nextPos >= verticesInCreationOrder.size()) {
				// already done, and we still get a report?
				// this can happen when:
				// - two job vertices finish almost simultaneously
				// - The first one advances the position for the second as well (second is in final state)
				// - the second (after it could grab the lock) tries to advance the position again
				return;
			}
			
			// see if we are the next to finish and then progress until the next unfinished one
			if (verticesInCreationOrder.get(nextPos) == ev) {
				do {
					nextPos++;
				}
				while (nextPos < verticesInCreationOrder.size() && verticesInCreationOrder.get(nextPos).isInFinalState());
				
				nextVertexToFinish = nextPos;
				
				if (nextPos == verticesInCreationOrder.size()) {
					
					// we are done, transition to the final state
					
					while (true) {
						JobStatus current = this.state;
						if (current == JobStatus.RUNNING && transitionState(current, JobStatus.FINISHED)) {
							break;
						}
						if (current == JobStatus.CANCELLING && transitionState(current, JobStatus.CANCELED)) {
							break;
						}
						if (current == JobStatus.FAILING && transitionState(current, JobStatus.FAILED, failureCause)) {
							break;
						}
						if (current == JobStatus.CANCELED || current == JobStatus.CREATED || current == JobStatus.FINISHED) {
							fail(new Exception("ExecutionGraph went into final state from state " + current));
						}
					}
					
					// also, notify waiters
					progressLock.notifyAll();
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Callbacks and Callback Utilities
	// --------------------------------------------------------------------------------------------
	
	public boolean updateState(TaskExecutionState state) {
		Execution attempt = this.currentExecutions.get(state.getID());
		if (attempt != null) {
			switch (state.getExecutionState()) {
				case FINISHED:
					attempt.markFinished();
					return true;
				case CANCELED:
					attempt.cancelingComplete();
					return true;
				case FAILED:
					attempt.markFailed(state.getError());
					return true;
				default:
					// we mark as failed and return false, which triggers the TaskManager
					// to remove the task
					attempt.fail(new Exception("TaskManager sent illegal state update: " + state.getExecutionState()));
					return false;
			}
		}
		else {
			return false;
		}
	}
	
	public ConnectionInfoLookupResponse lookupConnectionInfoAndDeployReceivers(InstanceConnectionInfo caller, ChannelID sourceChannelID) {
		
		final ExecutionEdge edge = edges.get(sourceChannelID);
		if (edge == null) {
			// that is bad, we need to fail the job
			LOG.error("Cannot find execution edge associated with ID " + sourceChannelID);
			fail(new Exception("Channels are not correctly registered"));
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}
		
		
		//  ----- Request was sent from an input channel (receiver side), requesting the output channel (sender side) ------
		//  -----                               This is the case for backwards events                                 ------

		if (sourceChannelID.equals(edge.getInputChannelId())) {
			final ExecutionVertex targetVertex = edge.getSource().getProducer();
			final ExecutionState executionState = targetVertex.getExecutionState();
			
			// common case - found task running
			if (executionState == ExecutionState.RUNNING) {
				Instance location = targetVertex.getCurrentAssignedResource().getInstance();
				
				if (location.getInstanceConnectionInfo().equals(caller)) {
					// Receiver runs on the same task manager
					return ConnectionInfoLookupResponse.createReceiverFoundAndReady(edge.getOutputChannelId());
				}
				else {
					// Receiver runs on a different task manager
					final InstanceConnectionInfo ici = location.getInstanceConnectionInfo();
					final InetSocketAddress isa = new InetSocketAddress(ici.address(), ici.dataPort());

					int connectionIdx = edge.getSource().getIntermediateResult().getConnectionIndex();
					return ConnectionInfoLookupResponse.createReceiverFoundAndReady(new RemoteReceiver(isa, connectionIdx));
				}
			}
			else if (executionState == ExecutionState.FINISHED) {
				// that should not happen. if there is data pending, the sender cannot yet be done
				// we need to fail the whole affair
				LOG.error("Receiver " + targetVertex + " set to FINISHED even though data is pending");
				fail(new Exception("Channels are not correctly registered"));
				return ConnectionInfoLookupResponse.createReceiverNotFound();
			}
			else if (executionState == ExecutionState.FAILED || executionState == ExecutionState.CANCELED ||
					executionState == ExecutionState.CANCELING)
			{
				return ConnectionInfoLookupResponse.createJobIsAborting();
			}
			else {
				// all other states should not be, because the sender cannot be in CREATED, SCHEDULED, or DEPLOYING
				// state when the receiver is already running
				LOG.error("Channel lookup (backwards) - sender " + targetVertex + " found in inconsistent state " + executionState);
				fail(new Exception("Channels are not correctly registered"));
				return ConnectionInfoLookupResponse.createReceiverNotFound();
			}
		}
		
		//  ----- Request was sent from an output channel (sender side), requesting the input channel (receiver side) ------
		//  -----                                 This is the case for forward data                                   ------
		
		final ExecutionVertex targetVertex = edge.getTarget();
		final ExecutionState executionState = targetVertex.getExecutionState();

		if (executionState == ExecutionState.RUNNING) {
			
			// already online
			Instance location = targetVertex.getCurrentAssignedResource().getInstance();
			
			if (location.getInstanceConnectionInfo().equals(caller)) {
				// Receiver runs on the same task manager
				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(edge.getInputChannelId());
			}
			else {
				// Receiver runs on a different task manager
				final InstanceConnectionInfo ici = location.getInstanceConnectionInfo();
				final InetSocketAddress isa = new InetSocketAddress(ici.address(), ici.dataPort());

				final int connectionIdx = edge.getSource().getIntermediateResult().getConnectionIndex();
				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(new RemoteReceiver(isa, connectionIdx));
			}
		}
		else if (executionState == ExecutionState.DEPLOYING || executionState == ExecutionState.SCHEDULED) {
			return ConnectionInfoLookupResponse.createReceiverNotReady();
		}
		else if (executionState == ExecutionState.CREATED) {
			// bring the receiver online
			try {
				edge.getTarget().scheduleForExecution(scheduler, false);
				
				// delay the requester
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}
			catch (JobException e) {
				fail(new Exception("Cannot schedule the receivers, not enough resources", e));
				return ConnectionInfoLookupResponse.createJobIsAborting();
			}
		}
		else if (executionState == ExecutionState.CANCELED || executionState == ExecutionState.CANCELING ||
				executionState == ExecutionState.FAILED)
		{
			return ConnectionInfoLookupResponse.createJobIsAborting();
		}
		else {
			// illegal state for all other states - or all the other state, since the only remaining state is FINISHED
			// state when the receiver is already running
			String message = "Channel lookup (forward) - receiver " + targetVertex + " found in inconsistent state " + executionState;
			LOG.error(message);
			fail(new Exception(message));
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}
	}
	
	public Map<ExecutionAttemptID, Execution> getRegisteredExecutions() {
		return Collections.unmodifiableMap(currentExecutions);
	}
	
	void registerExecution(Execution exec) {
		Execution previous = currentExecutions.putIfAbsent(exec.getAttemptId(), exec);
		if (previous != null) {
			fail(new Exception("Trying to register execution " + exec + " for already used ID " + exec.getAttemptId()));
		}
	}
	
	void deregisterExecution(Execution exec) {
		Execution contained = currentExecutions.remove(exec.getAttemptId());
		
		if (contained != null && contained != exec) {
			fail(new Exception("De-registering execution " + exec + " failed. Found for same ID execution " + contained));
		}
	}
	
	void registerExecutionEdge(ExecutionEdge edge) {
		ChannelID target = edge.getInputChannelId();
		ChannelID source = edge.getOutputChannelId();
		edges.put(source, edge);
		edges.put(target, edge);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Listeners & Observers
	// --------------------------------------------------------------------------------------------
	
	public void registerJobStatusListener(JobStatusListener jobStatusListener) {
		this.jobStatusListeners.add(jobStatusListener);
	}
	
	public void registerExecutionListener(ExecutionListener executionListener) {
		this.executionListeners.add(executionListener);
	}
	
	/**
	 * NOTE: This method never throws an error, only logs errors caused by the notified listeners.
	 * 
	 * @param newState
	 * @param error
	 */
	private void notifyJobStatusChange(JobStatus newState, Throwable error) {
		if (jobStatusListeners.size() > 0) {
			
			String message = error == null ? null : ExceptionUtils.stringifyException(error);
		
			for (JobStatusListener listener : this.jobStatusListeners) {
				try {
					listener.jobStatusHasChanged(this, newState, message);
				}
				catch (Throwable t) {
					LOG.error("Notification of job status change caused an error.", t);
				}
			}
		}
	}
	
	/**
	 * NOTE: This method never throws an error, only logs errors caused by the notified listeners.
	 * 
	 * @param vertexId
	 * @param subtask
	 * @param newExecutionState
	 * @param error
	 */
	void notifyExecutionChange(JobVertexID vertexId, int subtask, ExecutionAttemptID executionId, ExecutionState newExecutionState, Throwable error) {

		// we must be very careful here with exceptions 
		if (this.executionListeners.size() > 0) {
			
			String message = error == null ? null : ExceptionUtils.stringifyException(error);
			for (ExecutionListener listener : this.executionListeners) {
				try {
					listener.executionStateChanged(jobID, vertexId, subtask, executionId, newExecutionState, message);
				}
				catch (Throwable t) {
					LOG.error("Notification of execution state change caused an error.", t);
				}
			}
		}
		
		// see what this means for us. currently, the first FAILED state means -> FAILED
		if (newExecutionState == ExecutionState.FAILED) {
			fail(error);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	public void execute(Runnable action) {
		if (this.executor != null) {
			this.executor.submit(action);
		} else {
			action.run();
		}
	}
}

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
import org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
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
	
	/** The classloader of the user code. */
	private final ClassLoader userClassLoader;
	
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

	private final List<ActorRef> jobStatusListenerActors;

	private final List<ActorRef> executionListenerActors;
	
	private final long[] stateTimestamps;
	
	
	private final Object progressLock = new Object();
	
	private int nextVertexToFinish;
	
	private int numberOfRetriesLeft;
	
	private long delayBeforeRetrying;
	
	private volatile JobStatus state = JobStatus.CREATED;
	
	private volatile Throwable failureCause;
	
	
	private Scheduler scheduler;
	
	private boolean allowQueuedScheduling = true;
	
	
	public ExecutionGraph(JobID jobId, String jobName, Configuration jobConfig) {
		this(jobId, jobName, jobConfig, new ArrayList<BlobKey>(), null);
	}
	
	public ExecutionGraph(JobID jobId, String jobName, Configuration jobConfig,
						List<BlobKey> requiredJarFiles, ExecutorService executor) {
		this(jobId, jobName, jobConfig, requiredJarFiles, Thread.currentThread().getContextClassLoader(), null);
	}
	
	public ExecutionGraph(JobID jobId, String jobName, Configuration jobConfig, 
			List<BlobKey> requiredJarFiles, ClassLoader userClassLoader, ExecutorService executor) {
		if (jobId == null || jobName == null || jobConfig == null || userClassLoader == null) {
			throw new NullPointerException();
		}
		
		this.jobID = jobId;
		this.jobName = jobName;
		this.jobConfiguration = jobConfig;
		this.userClassLoader = userClassLoader;
		this.executor = executor;
		
		this.tasks = new ConcurrentHashMap<JobVertexID, ExecutionJobVertex>();
		this.intermediateResults = new ConcurrentHashMap<IntermediateDataSetID, IntermediateResult>();
		this.verticesInCreationOrder = new ArrayList<ExecutionJobVertex>();
		this.currentExecutions = new ConcurrentHashMap<ExecutionAttemptID, Execution>();
		
		this.jobStatusListeners = new CopyOnWriteArrayList<JobStatusListener>();
		this.executionListeners = new CopyOnWriteArrayList<ExecutionListener>();
		this.jobStatusListenerActors  = new CopyOnWriteArrayList<ActorRef>();
		this.executionListenerActors = new CopyOnWriteArrayList<ActorRef>();
		
		this.stateTimestamps = new long[JobStatus.values().length];
		this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

		this.requiredJarFiles = requiredJarFiles;
	}

	// --------------------------------------------------------------------------------------------
	
	public void setNumberOfRetriesLeft(int numberOfRetriesLeft) {
		if (numberOfRetriesLeft < -1) {
			throw new IllegalArgumentException();
		}
		this.numberOfRetriesLeft = numberOfRetriesLeft;
	}
	
	public int getNumberOfRetriesLeft() {
		return numberOfRetriesLeft;
	}
	
	public void setDelayBeforeRetrying(long delayBeforeRetrying) {
		if (delayBeforeRetrying < 0) {
			throw new IllegalArgumentException("Delay before retry must be non-negative.");
		}
		this.delayBeforeRetrying = delayBeforeRetrying;
	}
	
	public long getDelayBeforeRetrying() {
		return delayBeforeRetrying;
	}
	
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
	
	public ClassLoader getUserClassLoader() {
		return this.userClassLoader;
	}
	
	public JobStatus getState() {
		return state;
	}
	
	public Throwable getFailureCause() {
		return failureCause;
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
	
	public void registerJobStatusListener(JobStatusListener jobStatusListener) {
		this.jobStatusListeners.add(jobStatusListener);
	}
	
	public void registerExecutionListener(ExecutionListener executionListener) {
		this.executionListeners.add(executionListener);
	}

	public void registerJobStatusListener(ActorRef listener){
		this.jobStatusListenerActors.add(listener);
	}

	public void registerExecutionListener(ActorRef listener){
		this.executionListenerActors.add(listener);
	}
	
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
			
			long now = System.currentTimeMillis();
			long deadline = timeout == 0 ? Long.MAX_VALUE : now + timeout;
			
			
			while (now < deadline && !state.isTerminalState()) {
				progressLock.wait(deadline - now);
				now = System.currentTimeMillis();
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
						if (current == JobStatus.FAILING) {
							if (numberOfRetriesLeft > 0 && transitionState(current, JobStatus.RESTARTING)) {
								numberOfRetriesLeft--;
								
								execute(new Runnable() {
									@Override
									public void run() {
										try {
											Thread.sleep(delayBeforeRetrying);
										} catch (InterruptedException e) {
											// should only happen on shutdown
										}
										restart();
									}
								});
								break;
							}
							else if (numberOfRetriesLeft <= 0 && transitionState(current, JobStatus.FAILED, failureCause)) {
								break;
							}
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

		for(ActorRef listener: jobStatusListenerActors){
			listener.tell(new ExecutionGraphMessages.JobStatusChanged(this, newState, message), ActorRef.noSender());
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
	void notifyExecutionChange(JobVertexID vertexId, int subtask, ExecutionState2 newExecutionState, String optionalMessage) {
		for (ExecutionListener listener : this.executionListeners) {
			try {
				listener.executionStateChanged(jobID, vertexId, subtask,null, newExecutionState, optionalMessage);
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
	
	public void restart() {
		try {
			if (state == JobStatus.FAILED) {
				transitionState(JobStatus.FAILED, JobStatus.RESTARTING);
			}
			synchronized (progressLock) {
				if (state != JobStatus.RESTARTING) {
					throw new IllegalStateException("Can only restart job from state restarting.");
				}
				if (scheduler == null) {
					throw new IllegalStateException("The execution graph has not been schedudled before - scheduler is null.");
				}
				
				this.currentExecutions.clear();
				this.edges.clear();
				
				for (ExecutionJobVertex jv : this.verticesInCreationOrder) {
					jv.resetForNewExecution();
				}
				
				for (int i = 0; i < stateTimestamps.length; i++) {
					stateTimestamps[i] = 0;
				}
				nextVertexToFinish = 0;
				transitionState(JobStatus.RESTARTING, JobStatus.CREATED);
			}
			
			scheduleForExecution(scheduler);
		}
		catch (Throwable t) {
			fail(t);
		}
	}
}
	
//	
//	/**
//	 * Applies the user defined settings to the execution graph.
//	 * 
//	 * @param temporaryGroupVertexMap
//	 *        mapping between job vertices and the corresponding group vertices.
//	 * @throws GraphConversionException
//	 *         thrown if an error occurs while applying the user settings.
//	 */
//	private void applyUserDefinedSettings(final HashMap<AbstractJobVertex, ExecutionGroupVertex> temporaryGroupVertexMap)
//			throws GraphConversionException {
//
//		// The check for cycles in the dependency chain for instance sharing is already checked in
//		// <code>submitJob</code> method of the job manager
//
//		// If there is no cycle, apply the settings to the corresponding group vertices
//		final Iterator<Map.Entry<AbstractJobVertex, ExecutionGroupVertex>> it = temporaryGroupVertexMap.entrySet()
//			.iterator();
//		while (it.hasNext()) {
//
//			final Map.Entry<AbstractJobVertex, ExecutionGroupVertex> entry = it.next();
//			final AbstractJobVertex jobVertex = entry.getKey();
//			if (jobVertex.getVertexToShareInstancesWith() != null) {
//
//				final AbstractJobVertex vertexToShareInstancesWith = jobVertex.getVertexToShareInstancesWith();
//				final ExecutionGroupVertex groupVertex = entry.getValue();
//				final ExecutionGroupVertex groupVertexToShareInstancesWith = temporaryGroupVertexMap
//					.get(vertexToShareInstancesWith);
//				groupVertex.shareInstancesWith(groupVertexToShareInstancesWith);
//			}
//		}
//
//		// Second, we create the number of execution vertices each group vertex is supposed to manage
//		Iterator<ExecutionGroupVertex> it2 = new ExecutionGroupVertexIterator(this, true, -1);
//		while (it2.hasNext()) {
//
//			final ExecutionGroupVertex groupVertex = it2.next();
//			if (groupVertex.isNumberOfMembersUserDefined()) {
//				groupVertex.createInitialExecutionVertices(groupVertex.getUserDefinedNumberOfMembers());
//			}
//		}
//
//		// Finally, apply the channel settings channel settings
//		it2 = new ExecutionGroupVertexIterator(this, true, -1);
//		while (it2.hasNext()) {
//
//			final ExecutionGroupVertex groupVertex = it2.next();
//			for (int i = 0; i < groupVertex.getNumberOfForwardLinks(); i++) {
//
//				final ExecutionGroupEdge edge = groupVertex.getForwardEdge(i);
//				if (edge.isChannelTypeUserDefined()) {
//					edge.changeChannelType(edge.getChannelType());
//				}
//
//				// Create edges between execution vertices
//				createExecutionEdgesForGroupEdge(edge);
//			}
//		}
//
//		// Repair the instance assignment after having changed the channel types
//		repairInstanceAssignment();
//
//		// Repair the instance sharing among different group vertices
//		repairInstanceSharing();
//
//		// Finally, repair the stages
//		repairStages();
//	}
//
//	/**
//	 * Sets up an execution graph from a job graph.
//	 * 
//	 * @param jobGraph
//	 *        the job graph to create the execution graph from
//	 * @param defaultParallelism
//	 *        defaultParallelism in case that nodes have no parallelism set
//	 * @throws GraphConversionException
//	 *         thrown if the job graph is not valid and no execution graph can be constructed from it
//	 */
//	private void constructExecutionGraph(final JobGraph jobGraph, final int defaultParallelism)
//			throws GraphConversionException {
//
//		// Clean up temporary data structures
//		final HashMap<AbstractJobVertex, ExecutionVertex> temporaryVertexMap = new HashMap<AbstractJobVertex, ExecutionVertex>();
//		final HashMap<AbstractJobVertex, ExecutionGroupVertex> temporaryGroupVertexMap = new HashMap<AbstractJobVertex, ExecutionGroupVertex>();
//
//		// Initially, create only one execution stage that contains all group vertices
//		final ExecutionStage initialExecutionStage = new ExecutionStage(this, 0);
//		this.stages.add(initialExecutionStage);
//
//		// Convert job vertices to execution vertices and initialize them
//		final AbstractJobVertex[] all = jobGraph.getAllJobVertices();
//		for (int i = 0; i < all.length; i++) {
//			if(all[i].getNumberOfSubtasks() == -1){
//				all[i].setNumberOfSubtasks(defaultParallelism);
//			}
//
//			final ExecutionVertex createdVertex = createVertex(all[i], initialExecutionStage);
//			temporaryVertexMap.put(all[i], createdVertex);
//			temporaryGroupVertexMap.put(all[i], createdVertex.getGroupVertex());
//		}
//
//		// Create initial edges between the vertices
//		createInitialGroupEdges(temporaryVertexMap);
//
//		// Now that an initial graph is built, apply the user settings
//		applyUserDefinedSettings(temporaryGroupVertexMap);
//
//		// Calculate the connection IDs
//		calculateConnectionIDs();
//
//		// Finally, construct the execution pipelines
//		reconstructExecutionPipelines();
//	}
//
//	private void createExecutionEdgesForGroupEdge(final ExecutionGroupEdge groupEdge) {
//
//		final ExecutionGroupVertex source = groupEdge.getSourceVertex();
//		final int indexOfOutputGate = groupEdge.getIndexOfOutputGate();
//		final ExecutionGroupVertex target = groupEdge.getTargetVertex();
//		final int indexOfInputGate = groupEdge.getIndexOfInputGate();
//
//		final Map<GateID, List<ExecutionEdge>> inputChannelMap = new HashMap<GateID, List<ExecutionEdge>>();
//
//		// Unwire the respective gate of the source vertices
//		final int currentNumberOfSourceNodes = source.getCurrentNumberOfGroupMembers();
//		for (int i = 0; i < currentNumberOfSourceNodes; ++i) {
//
//			final ExecutionVertex sourceVertex = source.getGroupMember(i);
//			final ExecutionGate outputGate = sourceVertex.getOutputGate(indexOfOutputGate);
//			if (outputGate == null) {
//				throw new IllegalStateException("wire: " + sourceVertex.getName()
//					+ " has no output gate with index " + indexOfOutputGate);
//			}
//
//			if (outputGate.getNumberOfEdges() > 0) {
//				throw new IllegalStateException("wire: wire called on source " + sourceVertex.getName() + " (" + i
//					+ "), but number of output channels is " + outputGate.getNumberOfEdges() + "!");
//			}
//
//			final int currentNumberOfTargetNodes = target.getCurrentNumberOfGroupMembers();
//			final List<ExecutionEdge> outputChannels = new ArrayList<ExecutionEdge>();
//
//			for (int j = 0; j < currentNumberOfTargetNodes; ++j) {
//
//				final ExecutionVertex targetVertex = target.getGroupMember(j);
//				final ExecutionGate inputGate = targetVertex.getInputGate(indexOfInputGate);
//				if (inputGate == null) {
//					throw new IllegalStateException("wire: " + targetVertex.getName()
//						+ " has no input gate with index " + indexOfInputGate);
//				}
//
//				if (inputGate.getNumberOfEdges() > 0 && i == 0) {
//					throw new IllegalStateException("wire: wire called on target " + targetVertex.getName() + " ("
//						+ j + "), but number of input channels is " + inputGate.getNumberOfEdges() + "!");
//				}
//
//				// Check if a wire is supposed to be created
//				if (DistributionPatternProvider.createWire(groupEdge.getDistributionPattern(),
//					i, j, currentNumberOfSourceNodes, currentNumberOfTargetNodes)) {
//
//					final ChannelID outputChannelID = new ChannelID();
//					final ChannelID inputChannelID = new ChannelID();
//
//					final ExecutionEdge edge = new ExecutionEdge(outputGate, inputGate, groupEdge, outputChannelID,
//						inputChannelID, outputGate.getNumberOfEdges(), inputGate.getNumberOfEdges());
//
//					this.edgeMap.put(outputChannelID, edge);
//					this.edgeMap.put(inputChannelID, edge);
//
//					outputChannels.add(edge);
//
//					List<ExecutionEdge> inputChannels = inputChannelMap.get(inputGate.getGateID());
//					if (inputChannels == null) {
//						inputChannels = new ArrayList<ExecutionEdge>();
//						inputChannelMap.put(inputGate.getGateID(), inputChannels);
//					}
//
//					inputChannels.add(edge);
//				}
//			}
//
//			outputGate.replaceAllEdges(outputChannels);
//		}
//
//		// Finally, set the channels for the input gates
//		final int currentNumberOfTargetNodes = target.getCurrentNumberOfGroupMembers();
//		for (int i = 0; i < currentNumberOfTargetNodes; ++i) {
//
//			final ExecutionVertex targetVertex = target.getGroupMember(i);
//			final ExecutionGate inputGate = targetVertex.getInputGate(indexOfInputGate);
//
//			final List<ExecutionEdge> inputChannels = inputChannelMap.get(inputGate.getGateID());
//			if (inputChannels == null) {
//				LOG.error("Cannot find input channels for gate ID " + inputGate.getGateID());
//				continue;
//			}
//
//			inputGate.replaceAllEdges(inputChannels);
//		}
//
//	}
//
//	/**
//	 * Creates the initial edges between the group vertices
//	 * 
//	 * @param vertexMap
//	 *        the temporary vertex map
//	 * @throws GraphConversionException
//	 *         if the initial wiring cannot be created
//	 */
//	private void createInitialGroupEdges(final HashMap<AbstractJobVertex, ExecutionVertex> vertexMap)
//			throws GraphConversionException {
//
//		Iterator<Map.Entry<AbstractJobVertex, ExecutionVertex>> it = vertexMap.entrySet().iterator();
//
//		while (it.hasNext()) {
//
//			final Map.Entry<AbstractJobVertex, ExecutionVertex> entry = it.next();
//			final AbstractJobVertex sjv = entry.getKey();
//			final ExecutionVertex sev = entry.getValue();
//			final ExecutionGroupVertex sgv = sev.getGroupVertex();
//
//			// First, build the group edges
//			for (int i = 0; i < sjv.getNumberOfForwardConnections(); ++i) {
//				final JobEdge edge = sjv.getForwardConnection(i);
//				final AbstractJobVertex tjv = edge.getConnectedVertex();
//
//				final ExecutionVertex tev = vertexMap.get(tjv);
//				final ExecutionGroupVertex tgv = tev.getGroupVertex();
//				// Use NETWORK as default channel type if nothing else is defined by the user
//				ChannelType channelType = edge.getChannelType();
//				boolean userDefinedChannelType = true;
//				if (channelType == null) {
//					userDefinedChannelType = false;
//					channelType = ChannelType.NETWORK;
//				}
//
//				final DistributionPattern distributionPattern = edge.getDistributionPattern();
//
//				// Connect the corresponding group vertices and copy the user settings from the job edge
//				final ExecutionGroupEdge groupEdge = sgv.wireTo(tgv, edge.getIndexOfInputGate(), i, channelType,
//					userDefinedChannelType,distributionPattern);
//
//				final ExecutionGate outputGate = new ExecutionGate(new GateID(), sev, groupEdge, false);
//				sev.insertOutputGate(i, outputGate);
//				final ExecutionGate inputGate = new ExecutionGate(new GateID(), tev, groupEdge, true);
//				tev.insertInputGate(edge.getIndexOfInputGate(), inputGate);
//			}
//		}
//	}
//
//	/**
//	 * Creates an execution vertex from a job vertex.
//	 * 
//	 * @param jobVertex
//	 *        the job vertex to create the execution vertex from
//	 * @param initialExecutionStage
//	 *        the initial execution stage all group vertices are added to
//	 * @return the new execution vertex
//	 * @throws GraphConversionException
//	 *         thrown if the job vertex is of an unknown subclass
//	 */
//	private ExecutionVertex createVertex(final AbstractJobVertex jobVertex, final ExecutionStage initialExecutionStage)
//			throws GraphConversionException {
//
//		// Create an initial execution vertex for the job vertex
//		final Class<? extends AbstractInvokable> invokableClass = jobVertex.getInvokableClass();
//		if (invokableClass == null) {
//			throw new GraphConversionException("JobVertex " + jobVertex.getID() + " (" + jobVertex.getName()
//				+ ") does not specify a task");
//		}
//
//		// Calculate the cryptographic signature of this vertex
//		final ExecutionSignature signature = ExecutionSignature.createSignature(jobVertex.getInvokableClass(),
//			jobVertex.getJobGraph().getJobID());
//
//		// Create a group vertex for the job vertex
//
//		ExecutionGroupVertex groupVertex = null;
//		try {
//			groupVertex = new ExecutionGroupVertex(jobVertex.getName(), jobVertex.getID(), this,
//				jobVertex.getNumberOfSubtasks(), jobVertex.getVertexToShareInstancesWith() != null ? true
//					: false, 0, jobVertex.getConfiguration(), signature,
//				invokableClass);
//		} catch (Throwable t) {
//			throw new GraphConversionException(t);
//		}
//
//		// Register input and output vertices separately
//		if (jobVertex instanceof AbstractJobInputVertex) {
//
//			final AbstractJobInputVertex jobInputVertex = (AbstractJobInputVertex) jobVertex;
//			
//			if (jobVertex instanceof InputFormatVertex) {
//				try {
//					// get a handle to the user code class loader
//					ClassLoader cl = LibraryCacheManager.getClassLoader(jobVertex.getJobGraph().getJobID());
//					
//					((InputFormatVertex) jobVertex).initializeInputFormatFromTaskConfig(cl);
//				}
//				catch (Throwable t) {
//					throw new GraphConversionException("Could not deserialize input format.", t);
//				}
//			}
//			
//			final Class<? extends InputSplit> inputSplitType = jobInputVertex.getInputSplitType();
//			
//			InputSplit[] inputSplits;
//
//			try {
//				inputSplits = jobInputVertex.getInputSplits(jobVertex.getNumberOfSubtasks());
//			}
//			catch (Throwable t) {
//				throw new GraphConversionException("Cannot compute input splits for " + groupVertex.getName(), t);
//			}
//
//			if (inputSplits == null) {
//				inputSplits = new InputSplit[0];
//			}
//			
//			LOG.info("Job input vertex " + jobVertex.getName() + " generated " + inputSplits.length + " input splits");
//
//			// assign input splits and type
//			groupVertex.setInputSplits(inputSplits);
//			groupVertex.setInputSplitType(inputSplitType);
//		}
//
//		if (jobVertex instanceof OutputFormatVertex){
//			final OutputFormatVertex jobOutputVertex = (OutputFormatVertex) jobVertex;
//			
//			try {
//				// get a handle to the user code class loader
//				ClassLoader cl = LibraryCacheManager.getClassLoader(jobVertex.getJobGraph().getJobID());
//				jobOutputVertex.initializeOutputFormatFromTaskConfig(cl);
//			}
//			catch (Throwable t) {
//				throw new GraphConversionException("Could not deserialize output format.", t);
//			}
//
//			OutputFormat<?> outputFormat = jobOutputVertex.getOutputFormat();
//			if (outputFormat != null && outputFormat instanceof InitializeOnMaster){
//				try {
//					((InitializeOnMaster) outputFormat).initializeGlobal(jobVertex.getNumberOfSubtasks());
//				}
//				catch (Throwable t) {
//					throw new GraphConversionException(t);
//				}
//			}
//		}
//
//		// Add group vertex to initial execution stage
//		initialExecutionStage.addStageMember(groupVertex);
//
//		final ExecutionVertex ev = new ExecutionVertex(this, groupVertex, jobVertex.getNumberOfForwardConnections(),
//			jobVertex.getNumberOfBackwardConnections());
//
//		// Assign initial instance to vertex (may be overwritten later on when user settings are applied)
//		ev.setAllocatedResource(new AllocatedResource(DummyInstance.createDummyInstance(), null));
//
//		return ev;
//	}
//
//	/**
//	 * Returns the number of input vertices registered with this execution graph.
//	 * 
//	 * @return the number of input vertices registered with this execution graph
//	 */
//	public int getNumberOfInputVertices() {
//		return this.stages.get(0).getNumberOfInputExecutionVertices();
//	}
//
//	/**
//	 * Returns the number of input vertices for the given stage.
//	 * 
//	 * @param stage
//	 *        the index of the execution stage
//	 * @return the number of input vertices for the given stage
//	 */
//	public int getNumberOfInputVertices(int stage) {
//		if (stage >= this.stages.size()) {
//			return 0;
//		}
//
//		return this.stages.get(stage).getNumberOfInputExecutionVertices();
//	}
//
//	/**
//	 * Returns the number of output vertices registered with this execution graph.
//	 * 
//	 * @return the number of output vertices registered with this execution graph
//	 */
//	public int getNumberOfOutputVertices() {
//		return this.stages.get(0).getNumberOfOutputExecutionVertices();
//	}
//
//	/**
//	 * Returns the number of output vertices for the given stage.
//	 * 
//	 * @param stage
//	 *        the index of the execution stage
//	 * @return the number of input vertices for the given stage
//	 */
//	public int getNumberOfOutputVertices(final int stage) {
//		if (stage >= this.stages.size()) {
//			return 0;
//		}
//
//		return this.stages.get(stage).getNumberOfOutputExecutionVertices();
//	}
//
//	/**
//	 * Returns the input vertex with the specified index.
//	 * 
//	 * @param index
//	 *        the index of the input vertex to return
//	 * @return the input vertex with the specified index or <code>null</code> if no input vertex with such an index
//	 *         exists
//	 */
//	public ExecutionVertex getInputVertex(final int index) {
//		return this.stages.get(0).getInputExecutionVertex(index);
//	}
//
//	/**
//	 * Returns the output vertex with the specified index.
//	 * 
//	 * @param index
//	 *        the index of the output vertex to return
//	 * @return the output vertex with the specified index or <code>null</code> if no output vertex with such an index
//	 *         exists
//	 */
//	public ExecutionVertex getOutputVertex(final int index) {
//		return this.stages.get(0).getOutputExecutionVertex(index);
//	}
//
//	/**
//	 * Returns the input vertex with the specified index for the given stage
//	 * 
//	 * @param stage
//	 *        the index of the stage
//	 * @param index
//	 *        the index of the input vertex to return
//	 * @return the input vertex with the specified index or <code>null</code> if no input vertex with such an index
//	 *         exists in that stage
//	 */
//	public ExecutionVertex getInputVertex(final int stage, final int index) {
//		try {
//			final ExecutionStage s = this.stages.get(stage);
//			if (s == null) {
//				return null;
//			}
//
//			return s.getInputExecutionVertex(index);
//
//		} catch (ArrayIndexOutOfBoundsException e) {
//			return null;
//		}
//	}
//
//	/**
//	 * Returns the output vertex with the specified index for the given stage.
//	 * 
//	 * @param stage
//	 *        the index of the stage
//	 * @param index
//	 *        the index of the output vertex to return
//	 * @return the output vertex with the specified index or <code>null</code> if no output vertex with such an index
//	 *         exists in that stage
//	 */
//	public ExecutionVertex getOutputVertex(final int stage, final int index) {
//		try {
//			final ExecutionStage s = this.stages.get(stage);
//			if (s == null) {
//				return null;
//			}
//
//			return s.getOutputExecutionVertex(index);
//
//		} catch (ArrayIndexOutOfBoundsException e) {
//			return null;
//		}
//	}
//
//	/**
//	 * Identifies an execution by the specified channel ID and returns it.
//	 * 
//	 * @param id
//	 *        the channel ID to identify the vertex with
//	 * @return the execution vertex which has a channel with ID <code>id</code> or <code>null</code> if no such vertex
//	 *         exists in the execution graph
//	 */
//	public ExecutionVertex getVertexByChannelID(final ChannelID id) {
//		final ExecutionEdge edge = this.edgeMap.get(id);
//		if (edge == null) {
//			return null;
//		}
//
//		if (id.equals(edge.getOutputChannelID())) {
//			return edge.getOutputGate().getVertex();
//		}
//
//		return edge.getInputGate().getVertex();
//	}
//
//	/**
//	 * Finds an {@link ExecutionEdge} by its ID and returns it.
//	 * 
//	 * @param id
//	 *        the channel ID to identify the edge
//	 * @return the edge whose ID matches <code>id</code> or <code>null</code> if no such edge is known
//	 */
//	public ExecutionEdge getEdgeByID(final ChannelID id) {
//		return this.edgeMap.get(id);
//	}
//
//	/**
//	 * Registers an execution vertex with the execution graph.
//	 * 
//	 * @param vertex
//	 *        the execution vertex to register
//	 */
//	void registerExecutionVertex(final ExecutionVertex vertex) {
//		if (this.vertexMap.put(vertex.getID(), vertex) != null) {
//			throw new IllegalStateException("There is already an execution vertex with ID " + vertex.getID()
//				+ " registered");
//		}
//	}
//
//	/**
//	 * Returns the execution vertex with the given vertex ID.
//	 * 
//	 * @param id
//	 *        the vertex ID to retrieve the execution vertex
//	 * @return the execution vertex matching the provided vertex ID or <code>null</code> if no such vertex could be
//	 *         found
//	 */
//	public ExecutionVertex getVertexByID(final ExecutionVertexID id) {
//		return this.vertexMap.get(id);
//	}
//
//	/**
//	 * Checks if the current execution stage has been successfully completed, i.e.
//	 * all vertices in this stage have successfully finished their execution.
//	 * 
//	 * @return <code>true</code> if stage is completed, <code>false</code> otherwise
//	 */
//	private boolean isCurrentStageCompleted() {
//		if (this.indexToCurrentExecutionStage >= this.stages.size()) {
//			return true;
//		}
//
//		final ExecutionGraphIterator it = new ExecutionGraphIterator(this, this.indexToCurrentExecutionStage, true,
//			true);
//		while (it.hasNext()) {
//			final ExecutionVertex vertex = it.next();
//			if (vertex.getExecutionState() != ExecutionState.FINISHED) {
//				return false;
//			}
//		}
//
//		return true;
//	}
//
//	/**
//	 * Checks if the execution of execution graph is finished.
//	 * 
//	 * @return <code>true</code> if the execution of the graph is finished, <code>false</code> otherwise
//	 */
//	public boolean isExecutionFinished() {
//		return (getJobStatus() == InternalJobStatus.FINISHED);
//	}
//
//	/**
//	 * Returns the job ID of the job configuration this execution graph was originally constructed from.
//	 * 
//	 * @return the job ID of the job configuration this execution graph was originally constructed from
//	 */
//	public JobID getJobID() {
//		return this.jobID;
//	}
//
//	/**
//	 * Returns the index of the current execution stage.
//	 * 
//	 * @return the index of the current execution stage
//	 */
//	public int getIndexOfCurrentExecutionStage() {
//		return this.indexToCurrentExecutionStage;
//	}
//
//	/**
//	 * Returns the stage which is currently executed.
//	 * 
//	 * @return the currently executed stage or <code>null</code> if the job execution is already completed
//	 */
//	public ExecutionStage getCurrentExecutionStage() {
//
//		try {
//			return this.stages.get(this.indexToCurrentExecutionStage);
//		} catch (ArrayIndexOutOfBoundsException e) {
//			return null;
//		}
//	}
//
//	public void repairStages() {
//
//		final Map<ExecutionGroupVertex, Integer> stageNumbers = new HashMap<ExecutionGroupVertex, Integer>();
//		ExecutionGroupVertexIterator it = new ExecutionGroupVertexIterator(this, true, -1);
//
//		while (it.hasNext()) {
//
//			final ExecutionGroupVertex groupVertex = it.next();
//			int precedingNumber = 0;
//			if (stageNumbers.containsKey(groupVertex)) {
//				precedingNumber = stageNumbers.get(groupVertex).intValue();
//			} else {
//				stageNumbers.put(groupVertex, Integer.valueOf(precedingNumber));
//			}
//
//			for (int i = 0; i < groupVertex.getNumberOfForwardLinks(); i++) {
//
//				final ExecutionGroupEdge edge = groupVertex.getForwardEdge(i);
//				if (!stageNumbers.containsKey(edge.getTargetVertex())) {
//					// Target vertex has not yet been discovered
//					// Same stage as preceding vertex
//					stageNumbers.put(edge.getTargetVertex(), Integer.valueOf(precedingNumber));
//				} else {
//					final int stageNumber = stageNumbers.get(edge.getTargetVertex()).intValue();
//					if (stageNumber != precedingNumber) {
//						stageNumbers.put(edge.getTargetVertex(), (int) Math.max(precedingNumber, stageNumber));
//					}
//				}
//			}
//		}
//
//		// Traverse the graph backwards (starting from the output vertices) to make sure vertices are allocated in a
//		// stage as high as possible
//		it = new ExecutionGroupVertexIterator(this, false, -1);
//
//		while (it.hasNext()) {
//
//			final ExecutionGroupVertex groupVertex = it.next();
//			final int succeedingNumber = stageNumbers.get(groupVertex);
//
//			for (int i = 0; i < groupVertex.getNumberOfBackwardLinks(); i++) {
//
//				final ExecutionGroupEdge edge = groupVertex.getBackwardEdge(i);
//				final int stageNumber = stageNumbers.get(edge.getSourceVertex());
//				if (stageNumber != succeedingNumber) {
//					throw new IllegalStateException(edge.getSourceVertex() + " and " + edge.getTargetVertex()
//						+ " are assigned to different stages");
//				}
//			}
//		}
//
//		// Finally, assign the new stage numbers
//		this.stages.clear();
//		final Iterator<Map.Entry<ExecutionGroupVertex, Integer>> it2 = stageNumbers.entrySet().iterator();
//		while (it2.hasNext()) {
//
//			final Map.Entry<ExecutionGroupVertex, Integer> entry = it2.next();
//			final ExecutionGroupVertex groupVertex = entry.getKey();
//			final int stageNumber = entry.getValue().intValue();
//			// Prevent out of bounds exceptions
//			while (this.stages.size() <= stageNumber) {
//				this.stages.add(null);
//			}
//			ExecutionStage executionStage = this.stages.get(stageNumber);
//			// If the stage not yet exists,
//			if (executionStage == null) {
//				executionStage = new ExecutionStage(this, stageNumber);
//				this.stages.set(stageNumber, executionStage);
//			}
//
//			executionStage.addStageMember(groupVertex);
//			groupVertex.setExecutionStage(executionStage);
//		}
//	}
//
//	public void repairInstanceSharing() {
//
//		final Set<AllocatedResource> availableResources = new LinkedHashSet<AllocatedResource>();
//
//		final Iterator<ExecutionGroupVertex> it = new ExecutionGroupVertexIterator(this, true, -1);
//		while (it.hasNext()) {
//			final ExecutionGroupVertex groupVertex = it.next();
//			if (groupVertex.getVertexToShareInstancesWith() == null) {
//				availableResources.clear();
//				groupVertex.repairInstanceSharing(availableResources);
//			}
//		}
//	}
//
//	public void repairInstanceAssignment() {
//
//		Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);
//		while (it.hasNext()) {
//
//			final ExecutionVertex sourceVertex = it.next();
//
//			for (int i = 0; i < sourceVertex.getNumberOfOutputGates(); ++i) {
//
//				final ExecutionGate outputGate = sourceVertex.getOutputGate(i);
//				final ChannelType channelType = outputGate.getChannelType();
//				if (channelType == ChannelType.IN_MEMORY) {
//					final int numberOfOutputChannels = outputGate.getNumberOfEdges();
//					for (int j = 0; j < numberOfOutputChannels; ++j) {
//						final ExecutionEdge outputChannel = outputGate.getEdge(j);
//						outputChannel.getInputGate().getVertex()
//							.setAllocatedResource(sourceVertex.getAllocatedResource());
//					}
//				}
//			}
//		}
//
//		it = new ExecutionGraphIterator(this, false);
//		while (it.hasNext()) {
//
//			final ExecutionVertex targetVertex = it.next();
//
//			for (int i = 0; i < targetVertex.getNumberOfInputGates(); ++i) {
//
//				final ExecutionGate inputGate = targetVertex.getInputGate(i);
//				final ChannelType channelType = inputGate.getChannelType();
//				if (channelType == ChannelType.IN_MEMORY) {
//					final int numberOfInputChannels = inputGate.getNumberOfEdges();
//					for (int j = 0; j < numberOfInputChannels; ++j) {
//						final ExecutionEdge inputChannel = inputGate.getEdge(j);
//						inputChannel.getOutputGate().getVertex()
//							.setAllocatedResource(targetVertex.getAllocatedResource());
//					}
//				}
//			}
//		}
//	}
//
//	public ChannelType getChannelType(final ExecutionVertex sourceVertex, final ExecutionVertex targetVertex) {
//
//		final ExecutionGroupVertex sourceGroupVertex = sourceVertex.getGroupVertex();
//		final ExecutionGroupVertex targetGroupVertex = targetVertex.getGroupVertex();
//
//		final List<ExecutionGroupEdge> edges = sourceGroupVertex.getForwardEdges(targetGroupVertex);
//		if (edges.size() == 0) {
//			return null;
//		}
//
//		// On a task level, the two vertices are connected
//		final ExecutionGroupEdge edge = edges.get(0);
//
//		// Now lets see if these two concrete subtasks are connected
//		final ExecutionGate outputGate = sourceVertex.getOutputGate(edge.getIndexOfOutputGate());
//		for (int i = 0; i < outputGate.getNumberOfEdges(); ++i) {
//
//			final ExecutionEdge outputChannel = outputGate.getEdge(i);
//			if (targetVertex == outputChannel.getInputGate().getVertex()) {
//				return edge.getChannelType();
//			}
//		}
//
//		return null;
//	}
//
//	/**
//	 * Returns the job configuration that was originally attached to the job graph.
//	 * 
//	 * @return the job configuration that was originally attached to the job graph
//	 */
//	public Configuration getJobConfiguration() {
//		return this.jobConfiguration;
//	}
//
//	/**
//	 * Checks whether the job represented by the execution graph has the status <code>FINISHED</code>.
//	 * 
//	 * @return <code>true</code> if the job has the status <code>CREATED</code>, <code>false</code> otherwise
//	 */
//	private boolean jobHasFinishedStatus() {
//
//		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);
//
//		while (it.hasNext()) {
//
//			if (it.next().getExecutionState() != ExecutionState.FINISHED) {
//				return false;
//			}
//		}
//
//		return true;
//	}
//
//	/**
//	 * Checks whether the job represented by the execution graph has the status <code>SCHEDULED</code>.
//	 * 
//	 * @return <code>true</code> if the job has the status <code>SCHEDULED</code>, <code>false</code> otherwise
//	 */
//	private boolean jobHasScheduledStatus() {
//
//		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);
//
//		while (it.hasNext()) {
//
//			final ExecutionState s = it.next().getExecutionState();
//			if (s != ExecutionState.CREATED && s != ExecutionState.SCHEDULED && s != ExecutionState.READY) {
//				return false;
//			}
//		}
//
//		return true;
//	}
//
//	/**
//	 * Checks whether the job represented by the execution graph has the status <code>CANCELED</code> or
//	 * <code>FAILED</code>.
//	 * 
//	 * @return <code>true</code> if the job has the status <code>CANCELED</code> or <code>FAILED</code>,
//	 *         <code>false</code> otherwise
//	 */
//	private boolean jobHasFailedOrCanceledStatus() {
//
//		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(this, true);
//
//		while (it.hasNext()) {
//
//			final ExecutionState state = it.next().getExecutionState();
//
//			if (state != ExecutionState.CANCELED && state != ExecutionState.FAILED && state != ExecutionState.FINISHED) {
//				return false;
//			}
//		}
//
//		return true;
//	}
//
//	private static InternalJobStatus determineNewJobStatus(final ExecutionGraph eg,
//			final ExecutionState latestStateChange) {
//
//		final InternalJobStatus currentJobStatus = eg.getJobStatus();
//
//		switch (currentJobStatus) {
//		case CREATED:
//			if (eg.jobHasScheduledStatus()) {
//				return InternalJobStatus.SCHEDULED;
//			} else if (latestStateChange == ExecutionState.CANCELED) {
//				if (eg.jobHasFailedOrCanceledStatus()) {
//					return InternalJobStatus.CANCELED;
//				}
//			}else if(latestStateChange == ExecutionState.FAILED){
//				return InternalJobStatus.FAILING;
//			}
//			break;
//		case SCHEDULED:
//			if (latestStateChange == ExecutionState.RUNNING) {
//				return InternalJobStatus.RUNNING;
//			} else if (latestStateChange == ExecutionState.CANCELED) {
//				if (eg.jobHasFailedOrCanceledStatus()) {
//					return InternalJobStatus.CANCELED;
//				}
//			}else if(latestStateChange == ExecutionState.FAILED){
//				return InternalJobStatus.FAILING;
//			}
//			break;
//		case RUNNING:
//			if (latestStateChange == ExecutionState.CANCELED) {
//				return InternalJobStatus.CANCELING;
//			}
//			if (latestStateChange == ExecutionState.FAILED) {
//
//				final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
//				while (it.hasNext()) {
//
//					final ExecutionVertex vertex = it.next();
//					if (vertex.getExecutionState() == ExecutionState.FAILED) {
//						return InternalJobStatus.FAILING;
//					}
//				}
//			}
//			if (eg.jobHasFinishedStatus()) {
//				return InternalJobStatus.FINISHED;
//			}
//			break;
//		case FAILING:
//			if (eg.jobHasFailedOrCanceledStatus()) {
//				return InternalJobStatus.FAILED;
//			}
//			break;
//		case FAILED:
//			LOG.error("Received update of execute state in job status FAILED");
//			break;
//		case CANCELING:
//			if (eg.jobHasFailedOrCanceledStatus()) {
//				return InternalJobStatus.CANCELED;
//			}
//			break;
//		case CANCELED:
//			LOG.error("Received update of execute state in job status CANCELED: " + eg.getJobID());
//			break;
//		case FINISHED:
//			LOG.error("Received update of execute state in job status FINISHED: " + eg.getJobID() + " "
//				+ StringUtils.stringifyException(new Throwable()));
//			break;
//		}
//
//		return currentJobStatus;
//	}
//
//	/**
//	 * Returns the current status of the job
//	 * represented by this execution graph.
//	 * 
//	 * @return the current status of the job
//	 */
//	public InternalJobStatus getJobStatus() {
//
//		return this.jobStatus.get();
//	}
//
//
//	@Override
//	public void executionStateChanged(final JobID jobID, final ExecutionVertexID vertexID,
//			final ExecutionState newExecutionState, String optionalMessage) {
//
//		// Do not use the parameter newExecutionState here as it may already be out-dated
//
//		final ExecutionVertex vertex = getVertexByID(vertexID);
//		if (vertex == null) {
//			LOG.error("Cannot find execution vertex with the ID " + vertexID);
//			return;
//		}
//
//		final ExecutionState actualExecutionState = vertex.getExecutionState();
//
//		final InternalJobStatus newJobStatus = determineNewJobStatus(this, actualExecutionState);
//
//		if (actualExecutionState == ExecutionState.FINISHED) {
//			// It is worth checking if the current stage has complete
//			if (this.isCurrentStageCompleted()) {
//				// Increase current execution stage
//				++this.indexToCurrentExecutionStage;
//
//				if (this.indexToCurrentExecutionStage < this.stages.size()) {
//					final Iterator<ExecutionStageListener> it = this.executionStageListeners.iterator();
//					final ExecutionStage nextExecutionStage = getCurrentExecutionStage();
//					while (it.hasNext()) {
//						it.next().nextExecutionStageEntered(jobID, nextExecutionStage);
//					}
//				}
//			}
//		}
//
//		updateJobStatus(newJobStatus, optionalMessage);
//	}
//
//	/**
//	 * Updates the job status to given status and triggers the execution of the {@link JobStatusListener} objects.
//	 * 
//	 * @param newJobStatus
//	 *        the new job status
//	 * @param optionalMessage
//	 *        an optional message providing details on the reasons for the state change
//	 */
//	public void updateJobStatus(final InternalJobStatus newJobStatus, String optionalMessage) {
//
//		// Check if the new job status equals the old one
//		if (this.jobStatus.getAndSet(newJobStatus) == newJobStatus) {
//			return;
//		}
//
//		// The task caused the entire job to fail, save the error description
//		if (newJobStatus == InternalJobStatus.FAILING) {
//			this.errorDescription = optionalMessage;
//		}
//
//		// If this is the final failure state change, reuse the saved error description
//		if (newJobStatus == InternalJobStatus.FAILED) {
//			optionalMessage = this.errorDescription;
//		}
//
//		final Iterator<JobStatusListener> it = this.jobStatusListeners.iterator();
//		while (it.hasNext()) {
//			it.next().jobStatusHasChanged(this, newJobStatus, optionalMessage);
//		}
//	}
//
//	/**
//	 * Registers a new {@link JobStatusListener} object with this execution graph.
//	 * After being registered the object will receive notifications about changes
//	 * of the job status. It is not possible to register the same listener object
//	 * twice.
//	 * 
//	 * @param jobStatusListener
//	 *        the listener object to register
//	 */
//	public void registerJobStatusListener(final JobStatusListener jobStatusListener) {
//
//		if (jobStatusListener == null) {
//			throw new IllegalArgumentException("Argument jobStatusListener must not be null");
//		}
//
//		this.jobStatusListeners.addIfAbsent(jobStatusListener);
//	}
//
//	/**
//	 * Unregisters the given {@link JobStatusListener} object. After having called this
//	 * method, the object will no longer receive notifications about changes of the job
//	 * status.
//	 * 
//	 * @param jobStatusListener
//	 *        the listener object to unregister
//	 */
//	public void unregisterJobStatusListener(final JobStatusListener jobStatusListener) {
//
//		if (jobStatusListener == null) {
//			throw new IllegalArgumentException("Argument jobStatusListener must not be null");
//		}
//
//		this.jobStatusListeners.remove(jobStatusListener);
//	}
//
//	/**
//	 * Registers a new {@link ExecutionStageListener} object with this execution graph. After being registered the
//	 * object will receive a notification whenever the job has entered its next execution stage. Note that a
//	 * notification is not sent when the job has entered its initial execution stage.
//	 * 
//	 * @param executionStageListener
//	 *        the listener object to register
//	 */
//	public void registerExecutionStageListener(final ExecutionStageListener executionStageListener) {
//
//		if (executionStageListener == null) {
//			throw new IllegalArgumentException("Argument executionStageListener must not be null");
//		}
//
//		this.executionStageListeners.addIfAbsent(executionStageListener);
//	}
//
//	/**
//	 * Unregisters the given {@link ExecutionStageListener} object. After having called this method, the object will no
//	 * longer receiver notifications about the execution stage progress.
//	 * 
//	 * @param executionStageListener
//	 *        the listener object to unregister
//	 */
//	public void unregisterExecutionStageListener(final ExecutionStageListener executionStageListener) {
//
//		if (executionStageListener == null) {
//			throw new IllegalArgumentException("Argument executionStageListener must not be null");
//		}
//
//		this.executionStageListeners.remove(executionStageListener);
//	}
//
//	/**
//	 * Returns the name of the original job graph.
//	 * 
//	 * @return the name of the original job graph, possibly <code>null</code>
//	 */
//	public String getJobName() {
//		return this.jobName;
//	}
//	
//	private void calculateConnectionIDs() {
//		final Set<ExecutionGroupVertex> alreadyVisited = new HashSet<ExecutionGroupVertex>();
//		final ExecutionStage lastStage = getStage(getNumberOfStages() - 1);
//
//		for (int i = 0; i < lastStage.getNumberOfStageMembers(); ++i) {
//
//			final ExecutionGroupVertex groupVertex = lastStage.getStageMember(i);
//
//			int currentConnectionID = 0;
//
//			if (groupVertex.isOutputVertex()) {
//			currentConnectionID = groupVertex.calculateConnectionID(currentConnectionID, alreadyVisited);
//			}
//		}
//	}
//	
//	/**
//	 * Retrieves the number of required slots to run this execution graph
//	 * @return
//	 */
//	public int getRequiredSlots(){
//		int maxRequiredSlots = 0;
//
//		final Iterator<ExecutionStage> stageIterator = this.stages.iterator();
//
//		while(stageIterator.hasNext()){
//			final ExecutionStage stage = stageIterator.next();
//
//			int requiredSlots = stage.getRequiredSlots();
//
//			if(requiredSlots > maxRequiredSlots){
//				maxRequiredSlots = requiredSlots;
//			}
//		}
//
//		return maxRequiredSlots;
//	}
//}

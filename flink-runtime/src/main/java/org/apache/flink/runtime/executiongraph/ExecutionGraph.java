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

import akka.actor.ActorRef;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static akka.dispatch.Futures.future;

public class ExecutionGraph implements Serializable {

	private static final long serialVersionUID = 42L;

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
	private ClassLoader userClassLoader;

	/** All job vertices that are part of this graph */
	private final ConcurrentHashMap<JobVertexID, ExecutionJobVertex> tasks;

	/** All vertices, in the order in which they were created **/
	private final List<ExecutionJobVertex> verticesInCreationOrder;

	/** All intermediate results that are part of this graph */
	private final ConcurrentHashMap<IntermediateDataSetID, IntermediateResult> intermediateResults;

	/** The currently executed tasks, for callbacks */
	private final ConcurrentHashMap<ExecutionAttemptID, Execution> currentExecutions;

	private final List<BlobKey> requiredJarFiles;

	private final List<ActorRef> jobStatusListenerActors;

	private final List<ActorRef> executionListenerActors;

	private final long[] stateTimestamps;

	private final Object progressLock = new Object();

	private int nextVertexToFinish;

	private int numberOfRetriesLeft;

	private long delayBeforeRetrying;

	private final FiniteDuration timeout;

	private volatile JobStatus state = JobStatus.CREATED;

	private volatile Throwable failureCause;

	private Scheduler scheduler;

	private boolean allowQueuedScheduling = true;

	private ScheduleMode scheduleMode = ScheduleMode.FROM_SOURCES;

	
	public ExecutionGraph(JobID jobId, String jobName, Configuration jobConfig, FiniteDuration timeout) {
		this(jobId, jobName, jobConfig, timeout, new ArrayList<BlobKey>());
	}

	public ExecutionGraph(JobID jobId, String jobName, Configuration jobConfig,
						FiniteDuration timeout, List<BlobKey> requiredJarFiles) {
		this(jobId, jobName, jobConfig, timeout, requiredJarFiles, Thread.currentThread().getContextClassLoader());
	}

	public ExecutionGraph(JobID jobId, String jobName, Configuration jobConfig, FiniteDuration timeout,
			List<BlobKey> requiredJarFiles, ClassLoader userClassLoader) {

		if (jobId == null || jobName == null || jobConfig == null || userClassLoader == null) {
			throw new NullPointerException();
		}

		this.jobID = jobId;
		this.jobName = jobName;
		this.jobConfiguration = jobConfig;
		this.userClassLoader = userClassLoader;

		this.tasks = new ConcurrentHashMap<JobVertexID, ExecutionJobVertex>();
		this.intermediateResults = new ConcurrentHashMap<IntermediateDataSetID, IntermediateResult>();
		this.verticesInCreationOrder = new ArrayList<ExecutionJobVertex>();
		this.currentExecutions = new ConcurrentHashMap<ExecutionAttemptID, Execution>();

		this.jobStatusListenerActors  = new CopyOnWriteArrayList<ActorRef>();
		this.executionListenerActors = new CopyOnWriteArrayList<ActorRef>();

		this.stateTimestamps = new long[JobStatus.values().length];
		this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

		this.requiredJarFiles = requiredJarFiles;

		this.timeout = timeout;
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
			ExecutionJobVertex ejv = new ExecutionJobVertex(this, jobVertex, 1, timeout, createTimestamp);
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

	public Scheduler getScheduler() {
		return scheduler;
	}

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

	public void setScheduleMode(ScheduleMode scheduleMode) {
		this.scheduleMode = scheduleMode;
	}

	public ScheduleMode getScheduleMode() {
		return scheduleMode;
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	public void scheduleForExecution(Scheduler scheduler) throws JobException {
		if (scheduler == null) {
			throw new IllegalArgumentException("Scheduler must not be null.");
		}
		
		if (this.scheduler != null && this.scheduler != scheduler) {
			throw new IllegalArgumentException("Cannot use different schedulers for the same job");
		}
		
		if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
			this.scheduler = scheduler;

			switch (scheduleMode) {

				case FROM_SOURCES:
					// initially, we simply take the ones without inputs.
					// next, we implement the logic to go back from vertices that need computation
					// to the ones we need to start running
					for (ExecutionJobVertex ejv : this.tasks.values()) {
						if (ejv.getJobVertex().isInputVertex()) {
							ejv.scheduleAll(scheduler, allowQueuedScheduling);
						}
					}

					break;

				case ALL:
					for (ExecutionJobVertex ejv : getVerticesTopologically()) {
						ejv.scheduleAll(scheduler, allowQueuedScheduling);
					}

					break;

				case BACKTRACKING:
					throw new JobException("BACKTRACKING is currently not supported as schedule mode.");
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

				if (!verticesInCreationOrder.isEmpty()) {
					// cancel all. what is failed will not cancel but stay failed
					for (ExecutionJobVertex ejv : verticesInCreationOrder) {
						ejv.cancel();
					}
				} else {
					// set the state of the job to failed
					transitionState(JobStatus.FAILING, JobStatus.FAILED, t);
				}
				
				return;
			}
			
			// no need to treat other states
		}
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
								future(new Callable<Object>() {
									@Override
									public Object call() throws Exception {
										try{
											Thread.sleep(delayBeforeRetrying);
										}catch(InterruptedException e){
											// should only happen on shutdown
										}
										restart();
										return null;
									}
								}, AkkaUtils.globalExecutionContext());
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

	public void scheduleOrUpdateConsumers(ExecutionAttemptID executionId, int partitionIndex) {
		Execution execution = currentExecutions.get(executionId);


		if (execution == null) {
			fail(new IllegalStateException("Cannot find execution for execution ID " +
					executionId));
		}
		else if(execution.getVertex() == null){
			fail(new IllegalStateException("Execution with execution ID " + executionId +
				" has no vertex assigned."));
		} else {
			execution.getVertex().scheduleOrUpdateConsumers(partitionIndex);
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

	// --------------------------------------------------------------------------------------------
	//  Listeners & Observers
	// --------------------------------------------------------------------------------------------

	public void registerJobStatusListener(ActorRef listener){
		this.jobStatusListenerActors.add(listener);
	}

	public void registerExecutionListener(ActorRef listener){
		this.executionListenerActors.add(listener);
	}

	public boolean containsJobStatusListener(ActorRef listener) {
		return this.jobStatusListenerActors.contains(listener);
	}

	/**
	 * NOTE: This method never throws an error, only logs errors caused by the notified listeners.
	 */
	private void notifyJobStatusChange(JobStatus newState, Throwable error) {
		if(jobStatusListenerActors.size() > 0){
			for(ActorRef listener: jobStatusListenerActors){
				listener.tell(new ExecutionGraphMessages.JobStatusChanged(jobID, newState, System.currentTimeMillis(),
								error), ActorRef.noSender());
			}
		}
	}

	/**
	 * NOTE: This method never throws an error, only logs errors caused by the notified listeners.
	 */
	void notifyExecutionChange(JobVertexID vertexId, int subtask, ExecutionAttemptID executionID, ExecutionState
							newExecutionState, Throwable error) {
		ExecutionJobVertex vertex = getJobVertex(vertexId);

		if(executionListenerActors.size() >0){
			String message = error == null ? null : ExceptionUtils.stringifyException(error);
			for(ActorRef listener : executionListenerActors){
				listener.tell(new ExecutionGraphMessages.ExecutionStateChanged(jobID, vertexId,
								vertex.getJobVertex().getName(), vertex.getParallelism(), subtask,
								executionID, newExecutionState, System.currentTimeMillis(),
								message), ActorRef.noSender());
			}
		}

		// see what this means for us. currently, the first FAILED state means -> FAILED
		if (newExecutionState == ExecutionState.FAILED) {
			fail(error);
		}
	}

	public void restart() {
		try {
			if (state == JobStatus.FAILED) {
				if (!transitionState(JobStatus.FAILED, JobStatus.RESTARTING)) {
					throw new IllegalStateException("Execution Graph left the state FAILED while trying to restart.");
				}
			}
			
			synchronized (progressLock) {
				if (state != JobStatus.RESTARTING) {
					throw new IllegalStateException("Can only restart job from state restarting.");
				}
				if (scheduler == null) {
					throw new IllegalStateException("The execution graph has not been schedudled before - scheduler is null.");
				}

				this.currentExecutions.clear();

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
	
	/**
	 * This method cleans fields that are irrelevant for the archived execution attempt.
	 */
	public void prepareForArchiving() {
		if (!state.isTerminalState()) {
			throw new IllegalStateException("Can only archive the job from a terminal state");
		}
		
		userClassLoader = null;
		
		for (ExecutionJobVertex vertex : verticesInCreationOrder) {
			vertex.prepareForArchiving();
		}
		
		intermediateResults.clear();
		currentExecutions.clear();
		requiredJarFiles.clear();
		jobStatusListenerActors.clear();
		executionListenerActors.clear();
		
		scheduler = null;
	}
}

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

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.StreamCheckpointCoordinator;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;
import scala.concurrent.duration.Duration;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static akka.dispatch.Futures.future;

/**
 * The execution graph is the central data structure that coordinates the distributed
 * execution of a data flow. It keeps representations of each parallel task, each
 * intermediate result, and the communication between them.
 *
 * The execution graph consists of the following constructs:
 * <ul>
 *     <li>The {@link ExecutionJobVertex} represents one vertex from the JobGraph (usually one operation like
 *         "map" or "join") during execution. It holds the aggregated state of all parallel subtasks.
 *         The ExecutionJobVertex is identified inside the graph by the {@link JobVertexID}, which it takes
 *         from the JobGraph's corresponding JobVertex.</li>
 *     <li>The {@link ExecutionVertex} represents one parallel subtask. For each ExecutionJobVertex, there are
 *         as many ExecutionVertices as the parallelism. The ExecutionVertex is identified by
 *         the ExecutionJobVertex and the number of the parallel subtask</li>
 *     <li>The {@link Execution} is one attempt to execute a ExecutionVertex. There may be multiple Executions
 *         for the ExecutionVertex, in case of a failure, or in the case where some data needs to be recomputed
 *         because it is no longer available when requested by later operations. An Execution is always
 *         identified by an {@link ExecutionAttemptID}. All messages between the JobManager and the TaskManager
 *         about deployment of tasks and updates in the task status always use the ExecutionAttemptID to
 *         address the message receiver.</li>
 * </ul>
 *
 *
 */
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

	/** The classloader for the user code. Needed for calls into user code classes */
	private ClassLoader userClassLoader;

	/** All job vertices that are part of this graph */
	private final ConcurrentHashMap<JobVertexID, ExecutionJobVertex> tasks;

	/** All vertices, in the order in which they were created **/
	private final List<ExecutionJobVertex> verticesInCreationOrder;

	/** All intermediate results that are part of this graph */
	private final ConcurrentHashMap<IntermediateDataSetID, IntermediateResult> intermediateResults;

	/** The currently executed tasks, for callbacks */
	private final ConcurrentHashMap<ExecutionAttemptID, Execution> currentExecutions;

	/** A list of all libraries required during the job execution. Libraries have to be stored
	 * inside the BlobService and are referenced via the BLOB keys. */
	private final List<BlobKey> requiredJarFiles;

	private final List<ActorRef> jobStatusListenerActors;

	private final List<ActorRef> executionListenerActors;

	/** Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when
	 * the execution graph transitioned into a certain state. The index into this array is the
	 * ordinal of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is
	 * at {@code stateTimestamps[RUNNING.ordinal()]}. */
	private final long[] stateTimestamps;

	/** The lock used to secure all access to mutable fields, especially the tracking of progress
	 * within the job. */
	private final Object progressLock = new Object();

	/** The timeout for all messages that require a response/acknowledgement */
	private final FiniteDuration timeout;


	// ------ Configuration of the Execution -------

	/** The number of times failed executions should be retried. */
	private int numberOfRetriesLeft;

	/** The delay that the system should wait before restarting failed executions. */
	private long delayBeforeRetrying;

	/** Flag to indicate whether the scheduler may queue tasks for execution, or needs to be able
	 * to deploy them immediately. */
	private boolean allowQueuedScheduling = false;

	/** The mode of scheduling. Decides how to select the initial set of tasks to be deployed.
	 * May indicate to deploy all sources, or to deploy everything, or to deploy via backtracking
	 * from results than need to be materialized. */
	private ScheduleMode scheduleMode = ScheduleMode.FROM_SOURCES;


	// ------ Execution status and progress -------

	/** Current status of the job execution */
	private volatile JobStatus state = JobStatus.CREATED;

	/** The exception that caused the job to fail. This is set to the first root exception
	 * that was not recoverable and triggered job failure */
	private volatile Throwable failureCause;

	/** The scheduler to use for scheduling new tasks as they are needed */
	private Scheduler scheduler;

	/** The position of the vertex that is next expected to finish.
	 * This is an index into the "verticesInCreationOrder" collection.
	 * Once this value has reached the number of vertices, the job is done. */
	private int nextVertexToFinish;



	private ActorContext parentContext;

	private  ActorRef stateCheckpointerActor;

	private boolean checkpointingEnabled;

	private long checkpointingInterval = 5000;

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
	
	public void setStateCheckpointerActor(ActorRef stateCheckpointerActor) {
		this.stateCheckpointerActor = stateCheckpointerActor;
	}

	public ActorRef getStateCheckpointerActor() {
		return stateCheckpointerActor;
	}

	public void setParentContext(ActorContext parentContext) {
		this.parentContext = parentContext;
	}

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

	public void setCheckpointingEnabled(boolean checkpointingEnabled) {
		this.checkpointingEnabled = checkpointingEnabled;
	}

	public void setCheckpointingInterval(long checkpointingInterval) {
		this.checkpointingInterval = checkpointingInterval;
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

			if (checkpointingEnabled) {
				stateCheckpointerActor = StreamCheckpointCoordinator.spawn(parentContext, this,
						Duration.create(checkpointingInterval, TimeUnit.MILLISECONDS));
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
			if (LOG.isDebugEnabled()) {
				LOG.debug("{} switched from {} to {}.", this.getJobName(), current, newState);
			}

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
				case RUNNING:
					return attempt.switchToRunning();
				case FINISHED:
					attempt.markFinished();
					return true;
				case CANCELED:
					attempt.cancelingComplete();
					return true;
				case FAILED:
					attempt.markFailed(state.getError(userClassLoader));
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
	
	public void loadOperatorStates(Map<Tuple3<JobVertexID, Integer, Long> , StateHandle> states) {
		synchronized (this.progressLock) {
			for (Map.Entry<Tuple3<JobVertexID, Integer, Long>, StateHandle> state : states.entrySet())
				tasks.get(state.getKey()._1()).getTaskVertices()[state.getKey()._2()].setOperatorState(state.getValue());
		}
	}

	public void scheduleOrUpdateConsumers(ResultPartitionID partitionId) {

		final Execution execution = currentExecutions.get(partitionId.getProducerId());

		if (execution == null) {
			fail(new IllegalStateException("Cannot find execution for execution ID " +
					partitionId.getPartitionId()));
		}
		else if (execution.getVertex() == null){
			fail(new IllegalStateException("Execution with execution ID " +
					partitionId.getPartitionId() + " has no vertex assigned."));
		} else {
			execution.getVertex().scheduleOrUpdateConsumers(partitionId);
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
		if (jobStatusListenerActors.size() > 0) {
			ExecutionGraphMessages.JobStatusChanged message =
					new ExecutionGraphMessages.JobStatusChanged(jobID, newState, System.currentTimeMillis(), error);

			for (ActorRef listener: jobStatusListenerActors) {
				listener.tell(message, ActorRef.noSender());
			}
		}
	}

	/**
	 * NOTE: This method never throws an error, only logs errors caused by the notified listeners.
	 */
	void notifyExecutionChange(JobVertexID vertexId, int subtask, ExecutionAttemptID executionID, ExecutionState
							newExecutionState, Throwable error)
	{
		ExecutionJobVertex vertex = getJobVertex(vertexId);

		if (executionListenerActors.size() > 0) {
			String message = error == null ? null : ExceptionUtils.stringifyException(error);
			ExecutionGraphMessages.ExecutionStateChanged actorMessage =
					new ExecutionGraphMessages.ExecutionStateChanged(jobID, vertexId,  vertex.getJobVertex().getName(),
																	vertex.getParallelism(), subtask,
																	executionID, newExecutionState,
																	System.currentTimeMillis(), message);

			for (ActorRef listener : executionListenerActors) {
				listener.tell(actorMessage, ActorRef.noSender());
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
					throw new IllegalStateException("The execution graph has not been scheduled before - scheduler is null.");
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
		parentContext = null;
		stateCheckpointerActor = null;
	}
}

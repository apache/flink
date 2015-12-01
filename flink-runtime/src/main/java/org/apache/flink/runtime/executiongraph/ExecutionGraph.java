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

import akka.actor.ActorSystem;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.Savepoint;
import org.apache.flink.runtime.checkpoint.SavepointCoordinator;
import org.apache.flink.runtime.checkpoint.StateStore;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.SimpleCheckpointStatsTracker;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.UnrecoverableException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.runtime.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * <p>The ExecutionGraph implements {@link java.io.Serializable}, because it can be archived by
 * sending it to an archive actor via an actor message. The execution graph does contain some
 * non-serializable fields. These fields are not required in the archived form and are cleared
 * in the {@link #prepareForArchiving()} method.</p>
 */
public class ExecutionGraph implements Serializable {

	private static final long serialVersionUID = 42L;

	private static final AtomicReferenceFieldUpdater<ExecutionGraph, JobStatus> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(ExecutionGraph.class, JobStatus.class, "state");

	/** The log object used for debugging. */
	static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);
	
	// --------------------------------------------------------------------------------------------

	/** The lock used to secure all access to mutable fields, especially the tracking of progress
	 * within the job. */
	private final SerializableObject progressLock = new SerializableObject();

	/** The ID of the application this graph has been built for. This is
	 * generated when the graph is created and reset if necessary (currently
	 * only after {@link #restoreSavepoint(String)}). */
	private ApplicationID appId = new ApplicationID();

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

	/** A list of all libraries required during the job execution. Libraries have to be stored
	 * inside the BlobService and are referenced via the BLOB keys. */
	private final List<BlobKey> requiredJarFiles;

	/** A list of all classpaths required during the job execution. Classpaths have to be
	 * accessible on all nodes in the cluster. */
	private final List<URL> requiredClasspaths;

	/** Listeners that receive messages when the entire job switches it status (such as from
	 * RUNNING to FINISHED) */
	private final List<ActorGateway> jobStatusListenerActors;

	/** Listeners that receive messages whenever a single task execution changes its status */
	private final List<ActorGateway> executionListenerActors;

	/** Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when
	 * the execution graph transitioned into a certain state. The index into this array is the
	 * ordinal of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is
	 * at {@code stateTimestamps[RUNNING.ordinal()]}. */
	private final long[] stateTimestamps;

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

	/** Flag to indicate whether the Graph has been archived */
	private boolean isArchived = false;

	// ------ Execution status and progress. These values are volatile, and accessed under the lock -------

	/** Current status of the job execution */
	private volatile JobStatus state = JobStatus.CREATED;

	/** The exception that caused the job to fail. This is set to the first root exception
	 * that was not recoverable and triggered job failure */
	private volatile Throwable failureCause;

	/** The number of job vertices that have reached a terminal state */
	private volatile int numFinishedJobVertices;
	
	// ------ Fields that are relevant to the execution and need to be cleared before archiving  -------

	/** The scheduler to use for scheduling new tasks as they are needed */
	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	private Scheduler scheduler;

	/** The classloader for the user code. Needed for calls into user code classes */
	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	private ClassLoader userClassLoader;
	
	/** The coordinator for checkpoints, if snapshot checkpoints are enabled */
	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	private CheckpointCoordinator checkpointCoordinator;

	/** The coordinator for savepoints, if snapshot checkpoints are enabled */
	private transient SavepointCoordinator savepointCoordinator;

	/** Checkpoint stats tracker seperate from the coordinator in order to be
	 * available after archiving. */
	private CheckpointStatsTracker checkpointStatsTracker;

	/** The execution context which is used to execute futures. */
	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	private ExecutionContext executionContext;

	// ------ Fields that are only relevant for archived execution graphs ------------
	private ExecutionConfig executionConfig;

	private String jsonPlan;

	// --------------------------------------------------------------------------------------------
	//   Constructors
	// --------------------------------------------------------------------------------------------

	/**
	 * This constructor is for tests only, because it does not include class loading information.
	 */
	ExecutionGraph(
			ExecutionContext executionContext,
			JobID jobId,
			String jobName,
			Configuration jobConfig,
			FiniteDuration timeout) {
		this(
			executionContext,
			jobId,
			jobName,
			jobConfig,
			timeout,
			new ArrayList<BlobKey>(),
			new ArrayList<URL>(),
			ExecutionGraph.class.getClassLoader()
		);
	}

	public ExecutionGraph(
			ExecutionContext executionContext,
			JobID jobId,
			String jobName,
			Configuration jobConfig,
			FiniteDuration timeout,
			List<BlobKey> requiredJarFiles,
			List<URL> requiredClasspaths,
			ClassLoader userClassLoader) {

		if (executionContext == null || jobId == null || jobName == null || jobConfig == null || userClassLoader == null) {
			throw new NullPointerException();
		}

		this.executionContext = executionContext;

		this.jobID = jobId;
		this.jobName = jobName;
		this.jobConfiguration = jobConfig;
		this.userClassLoader = userClassLoader;

		this.tasks = new ConcurrentHashMap<JobVertexID, ExecutionJobVertex>();
		this.intermediateResults = new ConcurrentHashMap<IntermediateDataSetID, IntermediateResult>();
		this.verticesInCreationOrder = new ArrayList<ExecutionJobVertex>();
		this.currentExecutions = new ConcurrentHashMap<ExecutionAttemptID, Execution>();

		this.jobStatusListenerActors  = new CopyOnWriteArrayList<ActorGateway>();
		this.executionListenerActors = new CopyOnWriteArrayList<ActorGateway>();

		this.stateTimestamps = new long[JobStatus.values().length];
		this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

		this.requiredJarFiles = requiredJarFiles;
		this.requiredClasspaths = requiredClasspaths;

		this.timeout = timeout;
	}

	// --------------------------------------------------------------------------------------------
	//  Configuration of Data-flow wide execution settings  
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the number of job vertices currently held by this execution graph.
	 * @return The current number of job vertices.
	 */
	public int getNumberOfExecutionJobVertices() {
		return this.verticesInCreationOrder.size();
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

	public boolean isArchived() {
		return isArchived;
	}
	
	public void enableSnapshotCheckpointing(
			long interval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			List<ExecutionJobVertex> verticesToTrigger,
			List<ExecutionJobVertex> verticesToWaitFor,
			List<ExecutionJobVertex> verticesToCommitTo,
			ActorSystem actorSystem,
			UUID leaderSessionID,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			RecoveryMode recoveryMode,
			StateStore<Savepoint> savepointStore) throws Exception {

		// simple sanity checks
		if (interval < 10 || checkpointTimeout < 10) {
			throw new IllegalArgumentException();
		}
		if (state != JobStatus.CREATED) {
			throw new IllegalStateException("Job must be in CREATED state");
		}

		ExecutionVertex[] tasksToTrigger = collectExecutionVertices(verticesToTrigger);
		ExecutionVertex[] tasksToWaitFor = collectExecutionVertices(verticesToWaitFor);
		ExecutionVertex[] tasksToCommitTo = collectExecutionVertices(verticesToCommitTo);
		
		// disable to make sure existing checkpoint coordinators are cleared
		disableSnaphotCheckpointing();

		boolean isStatsDisabled = jobConfiguration.getBoolean(
				ConfigConstants.JOB_MANAGER_WEB_CHECKPOINTS_DISABLE,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_DISABLE);

		if (isStatsDisabled) {
			checkpointStatsTracker = new DisabledCheckpointStatsTracker();
		}
		else {
			int historySize = jobConfiguration.getInteger(
					ConfigConstants.JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE,
					ConfigConstants.DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE);

			checkpointStatsTracker = new SimpleCheckpointStatsTracker(historySize, tasksToWaitFor);
		}

		// create the coordinator that triggers and commits checkpoints and holds the state
		checkpointCoordinator = new CheckpointCoordinator(
				jobID,
				interval,
				checkpointTimeout,
				minPauseBetweenCheckpoints,
				maxConcurrentCheckpoints,
				tasksToTrigger,
				tasksToWaitFor,
				tasksToCommitTo,
				userClassLoader,
				checkpointIDCounter,
				completedCheckpointStore,
				recoveryMode,
				checkpointStatsTracker);
		
		// the periodic checkpoint scheduler is activated and deactivated as a result of
		// job status changes (running -> on, all other states -> off)
		registerJobStatusListener(
				checkpointCoordinator.createActivatorDeactivator(actorSystem, leaderSessionID));

		// Savepoint Coordinator
		savepointCoordinator = new SavepointCoordinator(
				appId,
				jobID,
				interval,
				checkpointTimeout,
				tasksToTrigger,
				tasksToWaitFor,
				tasksToCommitTo,
				userClassLoader,
				// Important: this counter needs to be shared with the periodic
				// checkpoint coordinator.
				checkpointIDCounter,
				savepointStore,
				checkpointStatsTracker);

		registerJobStatusListener(savepointCoordinator
				.createActivatorDeactivator(actorSystem, leaderSessionID));
	}

	/**
	 * Disables checkpointing.
	 *
	 * <p>The shutdown of the checkpoint coordinator might block. Make sure that calls to this
	 * method don't block the job manager actor and run asynchronously.
	 */
	public void disableSnaphotCheckpointing() throws Exception {
		if (state != JobStatus.CREATED) {
			throw new IllegalStateException("Job must be in CREATED state");
		}
		
		if (checkpointCoordinator != null) {
			checkpointCoordinator.shutdown();
			checkpointCoordinator = null;
			checkpointStatsTracker = null;
		}

		if (savepointCoordinator != null) {
			savepointCoordinator.shutdown();
			savepointCoordinator = null;
		}
	}

	public CheckpointCoordinator getCheckpointCoordinator() {
		return checkpointCoordinator;
	}

	public SavepointCoordinator getSavepointCoordinator() {
		return savepointCoordinator;
	}

	public CheckpointStatsTracker getCheckpointStatsTracker() {
		return checkpointStatsTracker;
	}

	private ExecutionVertex[] collectExecutionVertices(List<ExecutionJobVertex> jobVertices) {
		if (jobVertices.size() == 1) {
			ExecutionJobVertex jv = jobVertices.get(0);
			if (jv.getGraph() != this) {
				throw new IllegalArgumentException("Can only use ExecutionJobVertices of this ExecutionGraph");
			}
			return jv.getTaskVertices();
		}
		else {
			ArrayList<ExecutionVertex> all = new ArrayList<ExecutionVertex>();
			for (ExecutionJobVertex jv : jobVertices) {
				if (jv.getGraph() != this) {
					throw new IllegalArgumentException("Can only use ExecutionJobVertices of this ExecutionGraph");
				}
				all.addAll(Arrays.asList(jv.getTaskVertices()));
			}
			return all.toArray(new ExecutionVertex[all.size()]);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Properties and Status of the Execution Graph  
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns a list of BLOB keys referring to the JAR files required to run this job
	 * @return list of BLOB keys referring to the JAR files required to run this job
	 */
	public List<BlobKey> getRequiredJarFiles() {
		return this.requiredJarFiles;
	}

	/**
	 * Returns a list of classpaths referring to the directories/JAR files required to run this job
	 * @return list of classpaths referring to the directories/JAR files required to run this job
	 */
	public List<URL> getRequiredClasspaths() {
		return this.requiredClasspaths;
	}

	// --------------------------------------------------------------------------------------------

	public void setJsonPlan(String jsonPlan) {
		this.jsonPlan = jsonPlan;
	}

	public String getJsonPlan() {
		return jsonPlan;
	}

	public Scheduler getScheduler() {
		return scheduler;
	}

	public ApplicationID getApplicationID() {
		return appId;
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

	/**
	 * Returns the ExecutionContext associated with this ExecutionGraph.
	 *
	 * @return ExecutionContext associated with this ExecutionGraph
	 */
	public ExecutionContext getExecutionContext() {
		return executionContext;
	}

	/**
	 * Gets the internal flink accumulator map of maps which contains some metrics.
	 * @return A map of accumulators for every executed task.
	 */
	public Map<ExecutionAttemptID, Map<AccumulatorRegistry.Metric, Accumulator<?,?>>> getFlinkAccumulators() {
		Map<ExecutionAttemptID, Map<AccumulatorRegistry.Metric, Accumulator<?, ?>>> flinkAccumulators =
				new HashMap<ExecutionAttemptID, Map<AccumulatorRegistry.Metric, Accumulator<?, ?>>>();

		for (ExecutionVertex vertex : getAllExecutionVertices()) {
			Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> taskAccs = vertex.getCurrentExecutionAttempt().getFlinkAccumulators();
			flinkAccumulators.put(vertex.getCurrentExecutionAttempt().getAttemptId(), taskAccs);
		}

		return flinkAccumulators;
	}

	/**
	 * Merges all accumulator results from the tasks previously executed in the Executions.
	 * @return The accumulator map
	 */
	public Map<String, Accumulator<?,?>> aggregateUserAccumulators() {

		Map<String, Accumulator<?, ?>> userAccumulators = new HashMap<String, Accumulator<?, ?>>();

		for (ExecutionVertex vertex : getAllExecutionVertices()) {
			Map<String, Accumulator<?, ?>> next = vertex.getCurrentExecutionAttempt().getUserAccumulators();
			if (next != null) {
				AccumulatorHelper.mergeInto(userAccumulators, next);
			}
		}

		return userAccumulators;
	}

	/**
	 * Gets a serialized accumulator map.
	 * @return The accumulator map with serialized accumulator values.
	 * @throws IOException
	 */
	public Map<String, SerializedValue<Object>> getAccumulatorsSerialized() throws IOException {

		Map<String, Accumulator<?, ?>> accumulatorMap = aggregateUserAccumulators();

		Map<String, SerializedValue<Object>> result = new HashMap<String, SerializedValue<Object>>();
		for (Map.Entry<String, Accumulator<?, ?>> entry : accumulatorMap.entrySet()) {
			result.put(entry.getKey(), new SerializedValue<Object>(entry.getValue().getLocalValue()));
		}

		return result;
	}

	/**
	 * Returns the a stringified version of the user-defined accumulators.
	 * @return an Array containing the StringifiedAccumulatorResult objects
	 */
	public StringifiedAccumulatorResult[] getAccumulatorResultsStringified() {
		Map<String, Accumulator<?, ?>> accumulatorMap = aggregateUserAccumulators();
		return StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	public void attachJobGraph(List<JobVertex> topologiallySorted) throws JobException {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Attaching %d topologically sorted vertices to existing job graph with %d "
					+ "vertices and %d intermediate results.", topologiallySorted.size(), tasks.size(), intermediateResults.size()));
		}

		final long createTimestamp = System.currentTimeMillis();

		for (JobVertex jobVertex : topologiallySorted) {

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
					// simply take the vertices without inputs.
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
					// go back from vertices that need computation to the ones we need to run
					throw new JobException("BACKTRACKING is currently not supported as schedule mode.");
				default:
					throw new JobException("Schedule mode is invalid.");
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
			// Executions are being canceled. Go into cancelling and wait for
			// all vertices to be in their final state.
			else if (current == JobStatus.FAILING) {
				if (transitionState(current, JobStatus.CANCELLING)) {
					return;
				}
			}
			// All vertices have been cancelled and it's safe to directly go
			// into the canceled state.
			else if (current == JobStatus.RESTARTING) {
				synchronized (progressLock) {
					if (transitionState(current, JobStatus.CANCELED)) {
						postRunCleanup();
						progressLock.notifyAll();

						LOG.info("Canceled during restart.");
						return;
					}
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

	public void restart() {
		try {
			synchronized (progressLock) {
				JobStatus current = state;

				if (current == JobStatus.CANCELED) {
					LOG.info("Canceled job during restart. Aborting restart.");
					return;
				}
				else if (current != JobStatus.RESTARTING) {
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
				numFinishedJobVertices = 0;
				transitionState(JobStatus.RESTARTING, JobStatus.CREATED);
				
				// if we have checkpointed state, reload it into the executions
				if (checkpointCoordinator != null) {
					checkpointCoordinator.restoreLatestCheckpointedState(getAllVertices(), false, false);
				}
			}

			scheduleForExecution(scheduler);
		}
		catch (Throwable t) {
			fail(t);
		}
	}

	/**
	 * Restores the latest checkpointed state.
	 *
	 * <p>The recovery of checkpoints might block. Make sure that calls to this method don't
	 * block the job manager actor and run asynchronously.
	 * 
	 */
	public void restoreLatestCheckpointedState() throws Exception {
		synchronized (progressLock) {
			if (checkpointCoordinator != null) {
				checkpointCoordinator.restoreLatestCheckpointedState(getAllVertices(), false, false);
			}
		}
	}

	/**
	 * Restores the execution state back to a savepoint.
	 *
	 * <p>The execution vertices need to be in state {@link ExecutionState#CREATED} when calling
	 * this method. The operation might block. Make sure that calls don't block the job manager
	 * actor.
	 *
	 * <p><strong>Note</strong>: a call to this method changes the {@link #appId} of the execution
	 * graph if the operation is successful.
	 *
	 * @param savepointPath The path of the savepoint to rollback to.
	 * @throws IllegalStateException If checkpointing is disabled
	 * @throws IllegalStateException If checkpoint coordinator is shut down
	 * @throws Exception If failure during rollback
	 */
	public void restoreSavepoint(String savepointPath) throws Exception {
		synchronized (progressLock) {
			if (savepointCoordinator != null) {
				LOG.info("Restoring savepoint: " + savepointPath + ".");

				ApplicationID oldAppId = appId;
				this.appId = savepointCoordinator.restoreSavepoint(
						getAllVertices(), savepointPath);

				LOG.info("Set application ID to {} (from: {}).", appId, oldAppId);
			}
			else {
				// Sanity check
				throw new IllegalStateException("Checkpointing disabled.");
			}
		}
	}

	/**
	 * This method cleans fields that are irrelevant for the archived execution attempt.
	 */
	public void prepareForArchiving() {
		if (!state.isTerminalState()) {
			throw new IllegalStateException("Can only archive the job from a terminal state");
		}
		// "unpack" execution config before we throw away the usercode classloader.
		try {
			executionConfig = (ExecutionConfig) InstantiationUtil.readObjectFromConfig(jobConfiguration, ExecutionConfig.CONFIG_KEY,userClassLoader);
		} catch (Exception e) {
			LOG.warn("Error deserializing the execution config while archiving the execution graph", e);
		}
		// clear the non-serializable fields
		userClassLoader = null;
		scheduler = null;
		checkpointCoordinator = null;
		executionContext = null;

		for (ExecutionJobVertex vertex : verticesInCreationOrder) {
			vertex.prepareForArchiving();
		}

		intermediateResults.clear();
		currentExecutions.clear();
		requiredJarFiles.clear();
		jobStatusListenerActors.clear();
		executionListenerActors.clear();

		isArchived = true;
	}

	public ExecutionConfig getExecutionConfig() {
		return this.executionConfig;
	}

	/**
	 * For testing: This waits until the job execution has finished.
	 * @throws InterruptedException
	 */
	public void waitUntilFinished() throws InterruptedException {
		synchronized (progressLock) {
			while (!state.isTerminalState()) {
				progressLock.wait();
			}
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

	void jobVertexInFinalState() {
		synchronized (progressLock) {
			if (numFinishedJobVertices >= verticesInCreationOrder.size()) {
				throw new IllegalStateException("All vertices are already finished, cannot transition vertex to finished.");
			}

			numFinishedJobVertices++;
			
			if (numFinishedJobVertices == verticesInCreationOrder.size()) {
				
				// we are done, transition to the final state
				JobStatus current;
				while (true) {
					current = this.state;
					
					if (current == JobStatus.RUNNING) {
						if (transitionState(current, JobStatus.FINISHED)) {
							postRunCleanup();
							break;
						}
					}
					else if (current == JobStatus.CANCELLING) {
						if (transitionState(current, JobStatus.CANCELED)) {
							postRunCleanup();
							break;
						}
					}
					else if (current == JobStatus.FAILING) {
						boolean isRecoverable = !(failureCause instanceof UnrecoverableException);

						if (isRecoverable && numberOfRetriesLeft > 0 &&
								transitionState(current, JobStatus.RESTARTING)) {

							numberOfRetriesLeft--;
							
							if (delayBeforeRetrying > 0) {
								future(new Callable<Object>() {
									@Override
									public Object call() throws Exception {
										try {
											LOG.info("Delaying retry of job execution for {} ms ...", delayBeforeRetrying);
											Thread.sleep(delayBeforeRetrying);
										}
										catch(InterruptedException e){
											// should only happen on shutdown
										}
										restart();
										return null;
									}
								}, executionContext);
							} else {
								future(new Callable<Object>() {
									@Override
									public Object call() throws Exception {
										restart();
										return null;
									}
								}, executionContext);
							}
							break;
						}
						else if ((!isRecoverable || numberOfRetriesLeft <= 0) &&
								transitionState(current, JobStatus.FAILED, failureCause)) {

							postRunCleanup();
							break;
						}
					}
					else {
						fail(new Exception("ExecutionGraph went into final state from state " + current));
					}
				}
				// done transitioning the state

				// also, notify waiters
				progressLock.notifyAll();
			}
		}
	}

	private void postRunCleanup() {
		try {
			CheckpointCoordinator coord = this.checkpointCoordinator;
			this.checkpointCoordinator = null;
			if (coord != null) {
				coord.shutdown();
			}

			// We don't clean the checkpoint stats tracker, because we want
			// it to be available after the job has terminated.
		}
		catch (Exception e) {
			LOG.error("Error while cleaning up after execution", e);
		}

		try {
			CheckpointCoordinator coord = this.savepointCoordinator;
			this.savepointCoordinator = null;
			if (coord != null) {
				coord.shutdown();
			}
		}
		catch (Exception e) {
			LOG.error("Error while cleaning up after execution", e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Callbacks and Callback Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Updates the state of one of the ExecutionVertex's Execution attempts.
	 * If the new status if "FINISHED", this also updates the 
	 * 
	 * @param state The state update.
	 * @return True, if the task update was properly applied, false, if the execution attempt was not found.
	 */
	public boolean updateState(TaskExecutionState state) {
		Execution attempt = this.currentExecutions.get(state.getID());
		if (attempt != null) {

			switch (state.getExecutionState()) {
				case RUNNING:
					return attempt.switchToRunning();
				case FINISHED:
					try {
						AccumulatorSnapshot accumulators = state.getAccumulators();
						Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> flinkAccumulators =
							accumulators.deserializeFlinkAccumulators();
						Map<String, Accumulator<?, ?>> userAccumulators =
							accumulators.deserializeUserAccumulators(userClassLoader);
						attempt.markFinished(flinkAccumulators, userAccumulators);
					}
					catch (Exception e) {
						LOG.error("Failed to deserialize final accumulator results.", e);
						attempt.markFailed(e);
					}
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

	/**
	 * Updates the accumulators during the runtime of a job. Final accumulator results are transferred
	 * through the UpdateTaskExecutionState message.
	 * @param accumulatorSnapshot The serialized flink and user-defined accumulators
	 */
	public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
		Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> flinkAccumulators;
		Map<String, Accumulator<?, ?>> userAccumulators;
		try {
			flinkAccumulators = accumulatorSnapshot.deserializeFlinkAccumulators();
			userAccumulators = accumulatorSnapshot.deserializeUserAccumulators(userClassLoader);

			ExecutionAttemptID execID = accumulatorSnapshot.getExecutionAttemptID();
			Execution execution = currentExecutions.get(execID);
			if (execution != null) {
				execution.setAccumulators(flinkAccumulators, userAccumulators);
			} else {
				LOG.warn("Received accumulator result for unknown execution {}.", execID);
			}
		} catch (Exception e) {
			LOG.error("Cannot update accumulators for job {}.", jobID, e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Listeners & Observers
	// --------------------------------------------------------------------------------------------

	public void registerJobStatusListener(ActorGateway listener) {
		if (listener != null) {
			this.jobStatusListenerActors.add(listener);
		}
	}

	public void registerExecutionListener(ActorGateway listener) {
		if (listener != null) {
			this.executionListenerActors.add(listener);
		}
	}
	
	
	private void notifyJobStatusChange(JobStatus newState, Throwable error) {
		if (jobStatusListenerActors.size() > 0) {
			ExecutionGraphMessages.JobStatusChanged message =
					new ExecutionGraphMessages.JobStatusChanged(jobID, newState, System.currentTimeMillis(),
							error == null ? null : new SerializedThrowable(error));

			for (ActorGateway listener: jobStatusListenerActors) {
				listener.tell(message);
			}
		}
	}
	
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

			for (ActorGateway listener : executionListenerActors) {
				listener.tell(actorMessage);
			}
		}

		// see what this means for us. currently, the first FAILED state means -> FAILED
		if (newExecutionState == ExecutionState.FAILED) {
			fail(error);
		}
	}
}

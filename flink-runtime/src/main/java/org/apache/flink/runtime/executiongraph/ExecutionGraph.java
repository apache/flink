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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointFailureManager;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.FutureUtils.ConjunctFuture;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverTopology;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.restart.ExecutionGraphRestartCallback;
import org.apache.flink.runtime.executiongraph.restart.RestartCallback;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionTopology;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The execution graph is the central data structure that coordinates the distributed
 * execution of a data flow. It keeps representations of each parallel task, each
 * intermediate stream, and the communication between them.
 *
 * <p>The execution graph consists of the following constructs:
 * <ul>
 *     <li>The {@link ExecutionJobVertex} represents one vertex from the JobGraph (usually one operation like
 *         "map" or "join") during execution. It holds the aggregated state of all parallel subtasks.
 *         The ExecutionJobVertex is identified inside the graph by the {@link JobVertexID}, which it takes
 *         from the JobGraph's corresponding JobVertex.</li>
 *     <li>The {@link ExecutionVertex} represents one parallel subtask. For each ExecutionJobVertex, there are
 *         as many ExecutionVertices as the parallelism. The ExecutionVertex is identified by
 *         the ExecutionJobVertex and the index of the parallel subtask</li>
 *     <li>The {@link Execution} is one attempt to execute a ExecutionVertex. There may be multiple Executions
 *         for the ExecutionVertex, in case of a failure, or in the case where some data needs to be recomputed
 *         because it is no longer available when requested by later operations. An Execution is always
 *         identified by an {@link ExecutionAttemptID}. All messages between the JobManager and the TaskManager
 *         about deployment of tasks and updates in the task status always use the ExecutionAttemptID to
 *         address the message receiver.</li>
 * </ul>
 *
 * <h2>Global and local failover</h2>
 *
 * <p>The Execution Graph has two failover modes: <i>global failover</i> and <i>local failover</i>.
 *
 * <p>A <b>global failover</b> aborts the task executions for all vertices and restarts whole
 * data flow graph from the last completed checkpoint. Global failover is considered the
 * "fallback strategy" that is used when a local failover is unsuccessful, or when a issue is
 * found in the state of the ExecutionGraph that could mark it as inconsistent (caused by a bug).
 *
 * <p>A <b>local failover</b> is triggered when an individual vertex execution (a task) fails.
 * The local failover is coordinated by the {@link FailoverStrategy}. A local failover typically
 * attempts to restart as little as possible, but as much as necessary.
 *
 * <p>Between local- and global failover, the global failover always takes precedence, because it
 * is the core mechanism that the ExecutionGraph relies on to bring back consistency. The
 * guard that, the ExecutionGraph maintains a <i>global modification version</i>, which is incremented
 * with every global failover (and other global actions, like job cancellation, or terminal
 * failure). Local failover is always scoped by the modification version that the execution graph
 * had when the failover was triggered. If a new global modification version is reached during
 * local failover (meaning there is a concurrent global failover), the failover strategy has to
 * yield before the global failover.
 */
public class ExecutionGraph implements AccessExecutionGraph {

	/** The log object used for debugging. */
	static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

	// --------------------------------------------------------------------------------------------

	/** Job specific information like the job id, job name, job configuration, etc. */
	private final JobInformation jobInformation;

	/** Serialized job information or a blob key pointing to the offloaded job information. */
	private final Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey;

	/** The executor which is used to execute futures. */
	private final ScheduledExecutorService futureExecutor;

	/** The executor which is used to execute blocking io operations. */
	private final Executor ioExecutor;

	/** Executor that runs tasks in the job manager's main thread. */
	@Nonnull
	private ComponentMainThreadExecutor jobMasterMainThreadExecutor;

	/** {@code true} if all source tasks are stoppable. */
	private boolean isStoppable = true;

	/** All job vertices that are part of this graph. */
	private final Map<JobVertexID, ExecutionJobVertex> tasks;

	/** All vertices, in the order in which they were created. **/
	private final List<ExecutionJobVertex> verticesInCreationOrder;

	/** All intermediate results that are part of this graph. */
	private final Map<IntermediateDataSetID, IntermediateResult> intermediateResults;

	/** The currently executed tasks, for callbacks. */
	private final Map<ExecutionAttemptID, Execution> currentExecutions;

	/** Listeners that receive messages when the entire job switches it status
	 * (such as from RUNNING to FINISHED). */
	private final List<JobStatusListener> jobStatusListeners;

	/** The implementation that decides how to recover the failures of tasks. */
	private final FailoverStrategy failoverStrategy;

	/** Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when
	 * the execution graph transitioned into a certain state. The index into this array is the
	 * ordinal of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is
	 * at {@code stateTimestamps[RUNNING.ordinal()]}. */
	private final long[] stateTimestamps;

	/** The timeout for all messages that require a response/acknowledgement. */
	private final Time rpcTimeout;

	/** The timeout for slot allocations. */
	private final Time allocationTimeout;

	/** Strategy to use for restarts. */
	private final RestartStrategy restartStrategy;

	/** The slot provider strategy to use for allocating slots for tasks as they are needed. */
	private final SlotProviderStrategy slotProviderStrategy;

	/** The classloader for the user code. Needed for calls into user code classes. */
	private final ClassLoader userClassLoader;

	/** Registered KvState instances reported by the TaskManagers. */
	private final KvStateLocationRegistry kvStateLocationRegistry;

	/** Blob writer used to offload RPC messages. */
	private final BlobWriter blobWriter;

	private boolean legacyScheduling = true;

	/** The total number of vertices currently in the execution graph. */
	private int numVerticesTotal;

	private final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory;

	private PartitionReleaseStrategy partitionReleaseStrategy;

	private DefaultExecutionTopology executionTopology;

	@Nullable
	private InternalFailuresListener internalTaskFailuresListener;

	/** Counts all restarts. Used by other Gauges/Meters and does not register to metric group. */
	private final Counter numberOfRestartsCounter = new SimpleCounter();

	// ------ Configuration of the Execution -------

	/** The mode of scheduling. Decides how to select the initial set of tasks to be deployed.
	 * May indicate to deploy all sources, or to deploy everything, or to deploy via backtracking
	 * from results than need to be materialized. */
	private final ScheduleMode scheduleMode;

	/** The maximum number of prior execution attempts kept in history. */
	private final int maxPriorAttemptsHistoryLength;

	// ------ Execution status and progress. These values are volatile, and accessed under the lock -------

	private int verticesFinished;

	/** Current status of the job execution. */
	private volatile JobStatus state = JobStatus.CREATED;

	/** A future that completes once the job has reached a terminal state. */
	private final CompletableFuture<JobStatus> terminationFuture = new CompletableFuture<>();

	/** On each global recovery, this version is incremented. The version breaks conflicts
	 * between concurrent restart attempts by local failover strategies. */
	private long globalModVersion;

	/** The exception that caused the job to fail. This is set to the first root exception
	 * that was not recoverable and triggered job failure. */
	private Throwable failureCause;

	/** The extended failure cause information for the job. This exists in addition to 'failureCause',
	 * to let 'failureCause' be a strong reference to the exception, while this info holds no
	 * strong reference to any user-defined classes.*/
	private ErrorInfo failureInfo;

	private final JobMasterPartitionTracker partitionTracker;

	private final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker;

	/**
	 * Future for an ongoing or completed scheduling action.
	 */
	@Nullable
	private CompletableFuture<Void> schedulingFuture;

	// ------ Fields that are relevant to the execution and need to be cleared before archiving  -------

	/** The coordinator for checkpoints, if snapshot checkpoints are enabled. */
	@Nullable
	private CheckpointCoordinator checkpointCoordinator;

	/** TODO, replace it with main thread executor. */
	@Nullable
	private ScheduledExecutorService checkpointCoordinatorTimer;

	/** Checkpoint stats tracker separate from the coordinator in order to be
	 * available after archiving. */
	private CheckpointStatsTracker checkpointStatsTracker;

	// ------ Fields that are only relevant for archived execution graphs ------------
	@Nullable
	private String stateBackendName;

	private String jsonPlan;

	/** Shuffle master to register partitions for task deployment. */
	private final ShuffleMaster<?> shuffleMaster;

	// --------------------------------------------------------------------------------------------
	//   Constructors
	// --------------------------------------------------------------------------------------------

	public ExecutionGraph(
			JobInformation jobInformation,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			Time rpcTimeout,
			RestartStrategy restartStrategy,
			int maxPriorAttemptsHistoryLength,
			FailoverStrategy.Factory failoverStrategyFactory,
			SlotProvider slotProvider,
			ClassLoader userClassLoader,
			BlobWriter blobWriter,
			Time allocationTimeout,
			PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory,
			ShuffleMaster<?> shuffleMaster,
			JobMasterPartitionTracker partitionTracker,
			ScheduleMode scheduleMode) throws IOException {

		this.jobInformation = Preconditions.checkNotNull(jobInformation);

		this.blobWriter = Preconditions.checkNotNull(blobWriter);

		this.scheduleMode = checkNotNull(scheduleMode);

		this.jobInformationOrBlobKey = BlobWriter.serializeAndTryOffload(jobInformation, jobInformation.getJobId(), blobWriter);

		this.futureExecutor = Preconditions.checkNotNull(futureExecutor);
		this.ioExecutor = Preconditions.checkNotNull(ioExecutor);

		this.slotProviderStrategy = SlotProviderStrategy.from(
			scheduleMode,
			slotProvider,
			allocationTimeout);
		this.userClassLoader = Preconditions.checkNotNull(userClassLoader, "userClassLoader");

		this.tasks = new HashMap<>(16);
		this.intermediateResults = new HashMap<>(16);
		this.verticesInCreationOrder = new ArrayList<>(16);
		this.currentExecutions = new HashMap<>(16);

		this.jobStatusListeners  = new ArrayList<>();

		this.stateTimestamps = new long[JobStatus.values().length];
		this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

		this.rpcTimeout = checkNotNull(rpcTimeout);
		this.allocationTimeout = checkNotNull(allocationTimeout);

		this.partitionReleaseStrategyFactory = checkNotNull(partitionReleaseStrategyFactory);

		this.restartStrategy = restartStrategy;
		this.kvStateLocationRegistry = new KvStateLocationRegistry(jobInformation.getJobId(), getAllVertices());

		this.globalModVersion = 1L;

		// the failover strategy must be instantiated last, so that the execution graph
		// is ready by the time the failover strategy sees it
		this.failoverStrategy = checkNotNull(failoverStrategyFactory.create(this), "null failover strategy");

		this.maxPriorAttemptsHistoryLength = maxPriorAttemptsHistoryLength;

		this.schedulingFuture = null;
		this.jobMasterMainThreadExecutor =
			new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
				"ExecutionGraph is not initialized with proper main thread executor. " +
					"Call to ExecutionGraph.start(...) required.");

		this.shuffleMaster = checkNotNull(shuffleMaster);

		this.partitionTracker = checkNotNull(partitionTracker);

		this.resultPartitionAvailabilityChecker = new ExecutionGraphResultPartitionAvailabilityChecker(
			this::createResultPartitionId,
			partitionTracker);
	}

	public void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor) {
		this.jobMasterMainThreadExecutor = jobMasterMainThreadExecutor;
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

	public SchedulingTopology<?, ?> getSchedulingTopology() {
		return executionTopology;
	}

	public FailoverTopology<?, ?> getFailoverTopology() {
		return executionTopology;
	}

	public ScheduleMode getScheduleMode() {
		return scheduleMode;
	}

	public Time getAllocationTimeout() {
		return allocationTimeout;
	}

	@Nonnull
	public ComponentMainThreadExecutor getJobMasterMainThreadExecutor() {
		return jobMasterMainThreadExecutor;
	}

	@Override
	public boolean isArchived() {
		return false;
	}

	@Override
	public Optional<String> getStateBackendName() {
		return Optional.ofNullable(stateBackendName);
	}

	public void enableCheckpointing(
			CheckpointCoordinatorConfiguration chkConfig,
			List<ExecutionJobVertex> verticesToTrigger,
			List<ExecutionJobVertex> verticesToWaitFor,
			List<ExecutionJobVertex> verticesToCommitTo,
			List<MasterTriggerRestoreHook<?>> masterHooks,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore checkpointStore,
			StateBackend checkpointStateBackend,
			CheckpointStatsTracker statsTracker) {

		checkState(state == JobStatus.CREATED, "Job must be in CREATED state");
		checkState(checkpointCoordinator == null, "checkpointing already enabled");

		ExecutionVertex[] tasksToTrigger = collectExecutionVertices(verticesToTrigger);
		ExecutionVertex[] tasksToWaitFor = collectExecutionVertices(verticesToWaitFor);
		ExecutionVertex[] tasksToCommitTo = collectExecutionVertices(verticesToCommitTo);

		checkpointStatsTracker = checkNotNull(statsTracker, "CheckpointStatsTracker");

		CheckpointFailureManager failureManager = new CheckpointFailureManager(
			chkConfig.getTolerableCheckpointFailureNumber(),
			new CheckpointFailureManager.FailJobCallback() {
				@Override
				public void failJob(Throwable cause) {
					getJobMasterMainThreadExecutor().execute(() -> failGlobal(cause));
				}

				@Override
				public void failJobDueToTaskFailure(Throwable cause, ExecutionAttemptID failingTask) {
					getJobMasterMainThreadExecutor().execute(() -> failGlobalIfExecutionIsStillRunning(cause, failingTask));
				}
			}
		);

		checkState(checkpointCoordinatorTimer == null);

		checkpointCoordinatorTimer = Executors.newSingleThreadScheduledExecutor(
			new DispatcherThreadFactory(
				Thread.currentThread().getThreadGroup(), "Checkpoint Timer"));

		// create the coordinator that triggers and commits checkpoints and holds the state
		checkpointCoordinator = new CheckpointCoordinator(
			jobInformation.getJobId(),
			chkConfig,
			tasksToTrigger,
			tasksToWaitFor,
			tasksToCommitTo,
			checkpointIDCounter,
			checkpointStore,
			checkpointStateBackend,
			ioExecutor,
			new ScheduledExecutorServiceAdapter(checkpointCoordinatorTimer),
			SharedStateRegistry.DEFAULT_FACTORY,
			failureManager);

		// register the master hooks on the checkpoint coordinator
		for (MasterTriggerRestoreHook<?> hook : masterHooks) {
			if (!checkpointCoordinator.addMasterHook(hook)) {
				LOG.warn("Trying to register multiple checkpoint hooks with the name: {}", hook.getIdentifier());
			}
		}

		checkpointCoordinator.setCheckpointStatsTracker(checkpointStatsTracker);

		// interval of max long value indicates disable periodic checkpoint,
		// the CheckpointActivatorDeactivator should be created only if the interval is not max value
		if (chkConfig.getCheckpointInterval() != Long.MAX_VALUE) {
			// the periodic checkpoint scheduler is activated and deactivated as a result of
			// job status changes (running -> on, all other states -> off)
			registerJobStatusListener(checkpointCoordinator.createActivatorDeactivator());
		}

		this.stateBackendName = checkpointStateBackend.getClass().getSimpleName();
	}

	@Nullable
	public CheckpointCoordinator getCheckpointCoordinator() {
		return checkpointCoordinator;
	}

	public KvStateLocationRegistry getKvStateLocationRegistry() {
		return kvStateLocationRegistry;
	}

	public RestartStrategy getRestartStrategy() {
		return restartStrategy;
	}

	@Override
	public CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration() {
		if (checkpointStatsTracker != null) {
			return checkpointStatsTracker.getJobCheckpointingConfiguration();
		} else {
			return null;
		}
	}

	@Override
	public CheckpointStatsSnapshot getCheckpointStatsSnapshot() {
		if (checkpointStatsTracker != null) {
			return checkpointStatsTracker.createSnapshot();
		} else {
			return null;
		}
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
			ArrayList<ExecutionVertex> all = new ArrayList<>();
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

	public void setJsonPlan(String jsonPlan) {
		this.jsonPlan = jsonPlan;
	}

	@Override
	public String getJsonPlan() {
		return jsonPlan;
	}

	public SlotProviderStrategy getSlotProviderStrategy() {
		return slotProviderStrategy;
	}

	public Either<SerializedValue<JobInformation>, PermanentBlobKey> getJobInformationOrBlobKey() {
		return jobInformationOrBlobKey;
	}

	@Override
	public JobID getJobID() {
		return jobInformation.getJobId();
	}

	@Override
	public String getJobName() {
		return jobInformation.getJobName();
	}

	@Override
	public boolean isStoppable() {
		return this.isStoppable;
	}

	public Configuration getJobConfiguration() {
		return jobInformation.getJobConfiguration();
	}

	public ClassLoader getUserClassLoader() {
		return this.userClassLoader;
	}

	@Override
	public JobStatus getState() {
		return state;
	}

	public Throwable getFailureCause() {
		return failureCause;
	}

	public ErrorInfo getFailureInfo() {
		return failureInfo;
	}

	/**
	 * Gets the number of restarts, including full restarts and fine grained restarts.
	 * If a recovery is currently pending, this recovery is included in the count.
	 *
	 * @return The number of restarts so far
	 */
	public long getNumberOfRestarts() {
		return numberOfRestartsCounter.getCount();
	}

	@Override
	public ExecutionJobVertex getJobVertex(JobVertexID id) {
		return this.tasks.get(id);
	}

	@Override
	public Map<JobVertexID, ExecutionJobVertex> getAllVertices() {
		return Collections.unmodifiableMap(this.tasks);
	}

	@Override
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

	public int getTotalNumberOfVertices() {
		return numVerticesTotal;
	}

	public Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults() {
		return Collections.unmodifiableMap(this.intermediateResults);
	}

	@Override
	public Iterable<ExecutionVertex> getAllExecutionVertices() {
		return new Iterable<ExecutionVertex>() {
			@Override
			public Iterator<ExecutionVertex> iterator() {
				return new AllVerticesIterator(getVerticesTopologically().iterator());
			}
		};
	}

	@Override
	public long getStatusTimestamp(JobStatus status) {
		return this.stateTimestamps[status.ordinal()];
	}

	public final BlobWriter getBlobWriter() {
		return blobWriter;
	}

	/**
	 * Returns the ExecutionContext associated with this ExecutionGraph.
	 *
	 * @return ExecutionContext associated with this ExecutionGraph
	 */
	public Executor getFutureExecutor() {
		return futureExecutor;
	}

	/**
	 * Merges all accumulator results from the tasks previously executed in the Executions.
	 * @return The accumulator map
	 */
	public Map<String, OptionalFailure<Accumulator<?, ?>>> aggregateUserAccumulators() {

		Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = new HashMap<>();

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
	 */
	@Override
	public Map<String, SerializedValue<OptionalFailure<Object>>> getAccumulatorsSerialized() {
		return aggregateUserAccumulators()
			.entrySet()
			.stream()
			.collect(Collectors.toMap(
				Map.Entry::getKey,
				entry -> serializeAccumulator(entry.getKey(), entry.getValue())));
	}

	private static SerializedValue<OptionalFailure<Object>> serializeAccumulator(String name, OptionalFailure<Accumulator<?, ?>> accumulator) {
		try {
			if (accumulator.isFailure()) {
				return new SerializedValue<>(OptionalFailure.ofFailure(accumulator.getFailureCause()));
			}
			return new SerializedValue<>(OptionalFailure.of(accumulator.getUnchecked().getLocalValue()));
		} catch (IOException ioe) {
			LOG.error("Could not serialize accumulator " + name + '.', ioe);
			try {
				return new SerializedValue<>(OptionalFailure.ofFailure(ioe));
			} catch (IOException e) {
				throw new RuntimeException("It should never happen that we cannot serialize the accumulator serialization exception.", e);
			}
		}
	}

	/**
	 * Returns the a stringified version of the user-defined accumulators.
	 * @return an Array containing the StringifiedAccumulatorResult objects
	 */
	@Override
	public StringifiedAccumulatorResult[] getAccumulatorResultsStringified() {
		Map<String, OptionalFailure<Accumulator<?, ?>>> accumulatorMap = aggregateUserAccumulators();
		return StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);
	}

	public void enableNgScheduling(final InternalFailuresListener internalTaskFailuresListener) {
		checkNotNull(internalTaskFailuresListener);
		checkState(this.internalTaskFailuresListener == null, "enableNgScheduling can be only called once");
		this.internalTaskFailuresListener = internalTaskFailuresListener;
		this.legacyScheduling = false;
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	public void attachJobGraph(List<JobVertex> topologiallySorted) throws JobException {

		assertRunningInJobMasterMainThread();

		LOG.debug("Attaching {} topologically sorted vertices to existing job graph with {} " +
				"vertices and {} intermediate results.",
			topologiallySorted.size(),
			tasks.size(),
			intermediateResults.size());

		final ArrayList<ExecutionJobVertex> newExecJobVertices = new ArrayList<>(topologiallySorted.size());
		final long createTimestamp = System.currentTimeMillis();

		for (JobVertex jobVertex : topologiallySorted) {

			if (jobVertex.isInputVertex() && !jobVertex.isStoppable()) {
				this.isStoppable = false;
			}

			// create the execution job vertex and attach it to the graph
			ExecutionJobVertex ejv = new ExecutionJobVertex(
					this,
					jobVertex,
					1,
					maxPriorAttemptsHistoryLength,
					rpcTimeout,
					globalModVersion,
					createTimestamp);

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
			this.numVerticesTotal += ejv.getParallelism();
			newExecJobVertices.add(ejv);
		}

		// the topology assigning should happen before notifying new vertices to failoverStrategy
		executionTopology = new DefaultExecutionTopology(this);

		failoverStrategy.notifyNewVertices(newExecJobVertices);

		partitionReleaseStrategy = partitionReleaseStrategyFactory.createInstance(getSchedulingTopology());
	}

	public boolean isLegacyScheduling() {
		return legacyScheduling;
	}

	public void transitionToRunning() {
		if (!transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
			throw new IllegalStateException("Job may only be scheduled from state " + JobStatus.CREATED);
		}
	}

	public void scheduleForExecution() throws JobException {

		assertRunningInJobMasterMainThread();

		if (isLegacyScheduling()) {
			LOG.info("Job recovers via failover strategy: {}", failoverStrategy.getStrategyName());
		}

		final long currentGlobalModVersion = globalModVersion;

		if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {

			final CompletableFuture<Void> newSchedulingFuture = SchedulingUtils.schedule(
				scheduleMode,
				getAllExecutionVertices(),
				this);

			if (state == JobStatus.RUNNING && currentGlobalModVersion == globalModVersion) {
				schedulingFuture = newSchedulingFuture;
				newSchedulingFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

							if (!(strippedThrowable instanceof CancellationException)) {
								// only fail if the scheduling future was not canceled
								failGlobal(strippedThrowable);
							}
						}
					});
			} else {
				newSchedulingFuture.cancel(false);
			}
		}
		else {
			throw new IllegalStateException("Job may only be scheduled from state " + JobStatus.CREATED);
		}
	}

	public void cancel() {

		assertRunningInJobMasterMainThread();

		while (true) {
			JobStatus current = state;

			if (current == JobStatus.RUNNING || current == JobStatus.CREATED) {
				if (transitionState(current, JobStatus.CANCELLING)) {

					// make sure no concurrent local actions interfere with the cancellation
					final long globalVersionForRestart = incrementGlobalModVersion();

					final CompletableFuture<Void> ongoingSchedulingFuture = schedulingFuture;

					// cancel ongoing scheduling action
					if (ongoingSchedulingFuture != null) {
						ongoingSchedulingFuture.cancel(false);
					}

					final ConjunctFuture<Void> allTerminal = cancelVerticesAsync();
					allTerminal.whenComplete(
						(Void value, Throwable throwable) -> {
							if (throwable != null) {
								transitionState(
									JobStatus.CANCELLING,
									JobStatus.FAILED,
									new FlinkException(
										"Could not cancel job " + getJobName() + " because not all execution job vertices could be cancelled.",
										throwable));
							} else {
								// cancellations may currently be overridden by failures which trigger
								// restarts, so we need to pass a proper restart global version here
								allVerticesInTerminalState(globalVersionForRestart);
							}
						});

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
				if (transitionState(current, JobStatus.CANCELED)) {
					onTerminalState(JobStatus.CANCELED);

					LOG.info("Canceled during restart.");
					return;
				}
			}
			else {
				// no need to treat other states
				return;
			}
		}
	}

	private ConjunctFuture<Void> cancelVerticesAsync() {
		final ArrayList<CompletableFuture<?>> futures = new ArrayList<>(verticesInCreationOrder.size());

		// cancel all tasks (that still need cancelling)
		for (ExecutionJobVertex ejv : verticesInCreationOrder) {
			futures.add(ejv.cancelWithFuture());
		}

		// we build a future that is complete once all vertices have reached a terminal state
		return FutureUtils.waitForAll(futures);
	}

	/**
	 * Suspends the current ExecutionGraph.
	 *
	 * <p>The JobStatus will be directly set to {@link JobStatus#SUSPENDED} iff the current state is not a terminal
	 * state. All ExecutionJobVertices will be canceled and the onTerminalState() is executed.
	 *
	 * <p>The {@link JobStatus#SUSPENDED} state is a local terminal state which stops the execution of the job but does
	 * not remove the job from the HA job store so that it can be recovered by another JobManager.
	 *
	 * @param suspensionCause Cause of the suspension
	 */
	public void suspend(Throwable suspensionCause) {

		assertRunningInJobMasterMainThread();

		if (state.isTerminalState()) {
			// stay in a terminal state
			return;
		} else if (transitionState(state, JobStatus.SUSPENDED, suspensionCause)) {
			initFailureCause(suspensionCause);

			// make sure no concurrent local actions interfere with the cancellation
			incrementGlobalModVersion();

			// cancel ongoing scheduling action
			if (schedulingFuture != null) {
				schedulingFuture.cancel(false);
			}
			final ArrayList<CompletableFuture<Void>> executionJobVertexTerminationFutures = new ArrayList<>(verticesInCreationOrder.size());

			for (ExecutionJobVertex ejv: verticesInCreationOrder) {
				executionJobVertexTerminationFutures.add(ejv.suspend());
			}

			final ConjunctFuture<Void> jobVerticesTerminationFuture = FutureUtils.waitForAll(executionJobVertexTerminationFutures);

			checkState(jobVerticesTerminationFuture.isDone(), "Suspend needs to happen atomically");

			jobVerticesTerminationFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					if (throwable != null) {
						LOG.debug("Could not properly suspend the execution graph.", throwable);
					}

					onTerminalState(state);
					LOG.info("Job {} has been suspended.", getJobID());
				});
		} else {
			throw new IllegalStateException(String.format("Could not suspend because transition from %s to %s failed.", state, JobStatus.SUSPENDED));
		}
	}

	void failGlobalIfExecutionIsStillRunning(Throwable cause, ExecutionAttemptID failingAttempt) {
		final Execution failedExecution = currentExecutions.get(failingAttempt);
		if (failedExecution != null && failedExecution.getState() == ExecutionState.RUNNING) {
			failGlobal(cause);
		} else {
			LOG.debug("The failing attempt {} belongs to an already not" +
				" running task thus won't fail the job", failingAttempt);
		}
	}

	/**
	 * Fails the execution graph globally. This failure will not be recovered by a specific
	 * failover strategy, but results in a full restart of all tasks.
	 *
	 * <p>This global failure is meant to be triggered in cases where the consistency of the
	 * execution graph' state cannot be guaranteed any more (for example when catching unexpected
	 * exceptions that indicate a bug or an unexpected call race), and where a full restart is the
	 * safe way to get consistency back.
	 *
	 * @param t The exception that caused the failure.
	 */
	public void failGlobal(Throwable t) {
		if (!isLegacyScheduling()) {
			internalTaskFailuresListener.notifyGlobalFailure(t);
			return;
		}

		assertRunningInJobMasterMainThread();

		while (true) {
			JobStatus current = state;
			// stay in these states
			if (current == JobStatus.FAILING ||
				current == JobStatus.SUSPENDED ||
				current.isGloballyTerminalState()) {
				return;
			} else if (transitionState(current, JobStatus.FAILING, t)) {
				initFailureCause(t);

				// make sure no concurrent local or global actions interfere with the failover
				final long globalVersionForRestart = incrementGlobalModVersion();

				final CompletableFuture<Void> ongoingSchedulingFuture = schedulingFuture;

				// cancel ongoing scheduling action
				if (ongoingSchedulingFuture != null) {
					ongoingSchedulingFuture.cancel(false);
				}

				// we build a future that is complete once all vertices have reached a terminal state
				final ConjunctFuture<Void> allTerminal = cancelVerticesAsync();
				FutureUtils.assertNoException(allTerminal.handle(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							transitionState(
								JobStatus.FAILING,
								JobStatus.FAILED,
								new FlinkException("Could not cancel all execution job vertices properly.", throwable));
						} else {
							allVerticesInTerminalState(globalVersionForRestart);
						}
						return null;
					}));

				return;
			}

			// else: concurrent change to execution state, retry
		}
	}

	public void restart(long expectedGlobalVersion) {

		assertRunningInJobMasterMainThread();

		try {
			// check the global version to see whether this recovery attempt is still valid
			if (globalModVersion != expectedGlobalVersion) {
				LOG.info("Concurrent full restart subsumed this restart.");
				return;
			}

			final JobStatus current = state;

			if (current == JobStatus.CANCELED) {
				LOG.info("Canceled job during restart. Aborting restart.");
				return;
			} else if (current == JobStatus.FAILED) {
				LOG.info("Failed job during restart. Aborting restart.");
				return;
			} else if (current == JobStatus.SUSPENDED) {
				LOG.info("Suspended job during restart. Aborting restart.");
				return;
			} else if (current != JobStatus.RESTARTING) {
				throw new IllegalStateException("Can only restart job from state restarting.");
			}

			this.currentExecutions.clear();

			final Collection<CoLocationGroup> colGroups = new HashSet<>();
			final long resetTimestamp = System.currentTimeMillis();

			for (ExecutionJobVertex jv : this.verticesInCreationOrder) {

				CoLocationGroup cgroup = jv.getCoLocationGroup();
				if (cgroup != null && !colGroups.contains(cgroup)){
					cgroup.resetConstraints();
					colGroups.add(cgroup);
				}

				jv.resetForNewExecution(resetTimestamp, expectedGlobalVersion);
			}

			for (int i = 0; i < stateTimestamps.length; i++) {
				if (i != JobStatus.RESTARTING.ordinal()) {
					// Only clear the non restarting state in order to preserve when the job was
					// restarted. This is needed for the restarting time gauge
					stateTimestamps[i] = 0;
				}
			}

			transitionState(JobStatus.RESTARTING, JobStatus.CREATED);

			// if we have checkpointed state, reload it into the executions
			if (checkpointCoordinator != null) {
				checkpointCoordinator.restoreLatestCheckpointedState(getAllVertices(), false, false);
			}

			scheduleForExecution();
		}
		// TODO remove the catch block if we align the schematics to not fail global within the restarter.
		catch (Throwable t) {
			LOG.warn("Failed to restart the job.", t);
			failGlobal(t);
		}
	}

	/**
	 * Returns the serializable {@link ArchivedExecutionConfig}.
	 *
	 * @return ArchivedExecutionConfig which may be null in case of errors
	 */
	@Override
	public ArchivedExecutionConfig getArchivedExecutionConfig() {
		// create a summary of all relevant data accessed in the web interface's JobConfigHandler
		try {
			ExecutionConfig executionConfig = jobInformation.getSerializedExecutionConfig().deserializeValue(userClassLoader);
			if (executionConfig != null) {
				return executionConfig.archive();
			}
		} catch (IOException | ClassNotFoundException e) {
			LOG.error("Couldn't create ArchivedExecutionConfig for job {} ", getJobID(), e);
		}
		return null;
	}

	/**
	 * Returns the termination future of this {@link ExecutionGraph}. The termination future
	 * is completed with the terminal {@link JobStatus} once the ExecutionGraph reaches this
	 * terminal state and all {@link Execution} have been terminated.
	 *
	 * @return Termination future of this {@link ExecutionGraph}.
	 */
	public CompletableFuture<JobStatus> getTerminationFuture() {
		return terminationFuture;
	}

	@VisibleForTesting
	public JobStatus waitUntilTerminal() throws InterruptedException {
		try {
			return terminationFuture.get();
		}
		catch (ExecutionException e) {
			// this should never happen
			// it would be a bug, so we  don't expect this to be handled and throw
			// an unchecked exception here
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets the failover strategy used by the execution graph to recover from failures of tasks.
	 */
	public FailoverStrategy getFailoverStrategy() {
		return this.failoverStrategy;
	}

	/**
	 * Gets the current global modification version of the ExecutionGraph.
	 * The global modification version is incremented with each global action (cancel/fail/restart)
	 * and is used to disambiguate concurrent modifications between local and global
	 * failover actions.
	 */
	public long getGlobalModVersion() {
		return globalModVersion;
	}

	// ------------------------------------------------------------------------
	//  State Transitions
	// ------------------------------------------------------------------------

	private boolean transitionState(JobStatus current, JobStatus newState) {
		return transitionState(current, newState, null);
	}

	private void transitionState(JobStatus newState, Throwable error) {
		transitionState(state, newState, error);
	}

	private boolean transitionState(JobStatus current, JobStatus newState, Throwable error) {
		assertRunningInJobMasterMainThread();
		// consistency check
		if (current.isTerminalState()) {
			String message = "Job is trying to leave terminal state " + current;
			LOG.error(message);
			throw new IllegalStateException(message);
		}

		// now do the actual state transition
		if (state == current) {
			state = newState;
			LOG.info("Job {} ({}) switched from state {} to {}.", getJobName(), getJobID(), current, newState, error);

			stateTimestamps[newState.ordinal()] = System.currentTimeMillis();
			notifyJobStatusChange(newState, error);
			return true;
		}
		else {
			return false;
		}
	}

	private long incrementGlobalModVersion() {
		incrementRestarts();
		return ++globalModVersion;
	}

	public void incrementRestarts() {
		numberOfRestartsCounter.inc();
	}

	public void initFailureCause(Throwable t) {
		this.failureCause = t;
		this.failureInfo = new ErrorInfo(t, System.currentTimeMillis());
	}

	// ------------------------------------------------------------------------
	//  Job Status Progress
	// ------------------------------------------------------------------------

	/**
	 * Called whenever a vertex reaches state FINISHED (completed successfully).
	 * Once all vertices are in the FINISHED state, the program is successfully done.
	 */
	void vertexFinished() {
		assertRunningInJobMasterMainThread();
		final int numFinished = ++verticesFinished;
		if (numFinished == numVerticesTotal) {
			// done :-)

			// check whether we are still in "RUNNING" and trigger the final cleanup
			if (state == JobStatus.RUNNING) {
				// we do the final cleanup in the I/O executor, because it may involve
				// some heavier work

				try {
					for (ExecutionJobVertex ejv : verticesInCreationOrder) {
						ejv.getJobVertex().finalizeOnMaster(getUserClassLoader());
					}
				}
				catch (Throwable t) {
					ExceptionUtils.rethrowIfFatalError(t);
					failGlobal(new Exception("Failed to finalize execution on master", t));
					return;
				}

				// if we do not make this state transition, then a concurrent
				// cancellation or failure happened
				if (transitionState(JobStatus.RUNNING, JobStatus.FINISHED)) {
					onTerminalState(JobStatus.FINISHED);
				}
			}
		}
	}

	void vertexUnFinished() {
		assertRunningInJobMasterMainThread();
		verticesFinished--;
	}

	/**
	 * This method is a callback during cancellation/failover and called when all tasks
	 * have reached a terminal state (cancelled/failed/finished).
	 */
	private void allVerticesInTerminalState(long expectedGlobalVersionForRestart) {

		assertRunningInJobMasterMainThread();

		// we are done, transition to the final state
		JobStatus current;
		while (true) {
			current = this.state;

			if (current == JobStatus.RUNNING) {
				failGlobal(new Exception("ExecutionGraph went into allVerticesInTerminalState() from RUNNING"));
			}
			else if (current == JobStatus.CANCELLING) {
				if (transitionState(current, JobStatus.CANCELED)) {
					onTerminalState(JobStatus.CANCELED);
					break;
				}
			}
			else if (current == JobStatus.FAILING) {
				if (tryRestartOrFail(expectedGlobalVersionForRestart)) {
					break;
				}
				// concurrent job status change, let's check again
			}
			else if (current.isGloballyTerminalState()) {
				LOG.warn("Job has entered globally terminal state without waiting for all " +
					"job vertices to reach final state.");
				break;
			}
			else {
				failGlobal(new Exception("ExecutionGraph went into final state from state " + current));
				break;
			}
		}
		// done transitioning the state
	}

	/**
	 * Try to restart the job. If we cannot restart the job (e.g. no more restarts allowed), then
	 * try to fail the job. This operation is only permitted if the current state is FAILING or
	 * RESTARTING.
	 *
	 * @return true if the operation could be executed; false if a concurrent job status change occurred
	 */
	@Deprecated
	private boolean tryRestartOrFail(long globalModVersionForRestart) {
		if (!isLegacyScheduling()) {
			return true;
		}

		JobStatus currentState = state;

		if (currentState == JobStatus.FAILING || currentState == JobStatus.RESTARTING) {
			final Throwable failureCause = this.failureCause;

			if (LOG.isDebugEnabled()) {
				LOG.debug("Try to restart or fail the job {} ({}) if no longer possible.", getJobName(), getJobID(), failureCause);
			} else {
				LOG.info("Try to restart or fail the job {} ({}) if no longer possible.", getJobName(), getJobID());
			}

			final boolean isFailureCauseAllowingRestart = !(failureCause instanceof SuppressRestartsException);
			final boolean isRestartStrategyAllowingRestart = restartStrategy.canRestart();
			boolean isRestartable = isFailureCauseAllowingRestart && isRestartStrategyAllowingRestart;

			if (isRestartable && transitionState(currentState, JobStatus.RESTARTING)) {
				LOG.info("Restarting the job {} ({}).", getJobName(), getJobID());

				RestartCallback restarter = new ExecutionGraphRestartCallback(this, globalModVersionForRestart);
				FutureUtils.assertNoException(
					restartStrategy
						.restart(restarter, getJobMasterMainThreadExecutor())
						.exceptionally((throwable) -> {
								failGlobal(throwable);
								return null;
							}));
				return true;
			}
			else if (!isRestartable && transitionState(currentState, JobStatus.FAILED, failureCause)) {
				final String cause1 = isFailureCauseAllowingRestart ? null :
					"a type of SuppressRestartsException was thrown";
				final String cause2 = isRestartStrategyAllowingRestart ? null :
					"the restart strategy prevented it";

				LOG.info("Could not restart the job {} ({}) because {}.", getJobName(), getJobID(),
					StringUtils.concatenateWithAnd(cause1, cause2), failureCause);
				onTerminalState(JobStatus.FAILED);

				return true;
			} else {
				// we must have changed the state concurrently, thus we cannot complete this operation
				return false;
			}
		} else {
			// this operation is only allowed in the state FAILING or RESTARTING
			return false;
		}
	}

	public void failJob(Throwable cause) {
		if (state == JobStatus.FAILING || state.isGloballyTerminalState()) {
			return;
		}

		transitionState(JobStatus.FAILING, cause);
		initFailureCause(cause);

		FutureUtils.assertNoException(
			cancelVerticesAsync().whenComplete((aVoid, throwable) -> {
				transitionState(JobStatus.FAILED, cause);
				onTerminalState(JobStatus.FAILED);
			}));
	}

	private void onTerminalState(JobStatus status) {
		try {
			CheckpointCoordinator coord = this.checkpointCoordinator;
			this.checkpointCoordinator = null;
			if (coord != null) {
				coord.shutdown(status);
			}
			if (checkpointCoordinatorTimer != null) {
				checkpointCoordinatorTimer.shutdownNow();
				checkpointCoordinatorTimer = null;
			}
		}
		catch (Exception e) {
			LOG.error("Error while cleaning up after execution", e);
		}
		finally {
			terminationFuture.complete(status);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Callbacks and Callback Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Updates the state of one of the ExecutionVertex's Execution attempts.
	 * If the new status if "FINISHED", this also updates the accumulators.
	 *
	 * @param state The state update.
	 * @return True, if the task update was properly applied, false, if the execution attempt was not found.
	 */
	public boolean updateState(TaskExecutionState state) {
		assertRunningInJobMasterMainThread();
		final Execution attempt = currentExecutions.get(state.getID());

		if (attempt != null) {
			try {
				final boolean stateUpdated = updateStateInternal(state, attempt);
				maybeReleasePartitions(attempt);
				return stateUpdated;
			}
			catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

				// failures during updates leave the ExecutionGraph inconsistent
				failGlobal(t);
				return false;
			}
		}
		else {
			return false;
		}
	}

	private boolean updateStateInternal(final TaskExecutionState state, final Execution attempt) {
		Map<String, Accumulator<?, ?>> accumulators;

		switch (state.getExecutionState()) {
			case RUNNING:
				return attempt.switchToRunning();

			case FINISHED:
				// this deserialization is exception-free
				accumulators = deserializeAccumulators(state);
				attempt.markFinished(accumulators, state.getIOMetrics());
				return true;

			case CANCELED:
				// this deserialization is exception-free
				accumulators = deserializeAccumulators(state);
				attempt.completeCancelling(accumulators, state.getIOMetrics(), false);
				return true;

			case FAILED:
				// this deserialization is exception-free
				accumulators = deserializeAccumulators(state);
				attempt.markFailed(state.getError(userClassLoader), accumulators, state.getIOMetrics(), !isLegacyScheduling());
				return true;

			default:
				// we mark as failed and return false, which triggers the TaskManager
				// to remove the task
				attempt.fail(new Exception("TaskManager sent illegal state update: " + state.getExecutionState()));
				return false;
		}
	}

	private void maybeReleasePartitions(final Execution attempt) {
		final ExecutionVertexID finishedExecutionVertex = attempt.getVertex().getID();

		if (attempt.getState() == ExecutionState.FINISHED) {
			final List<IntermediateResultPartitionID> releasablePartitions = partitionReleaseStrategy.vertexFinished(finishedExecutionVertex);
			releasePartitions(releasablePartitions);
		} else {
			partitionReleaseStrategy.vertexUnfinished(finishedExecutionVertex);
		}
	}

	private void releasePartitions(final List<IntermediateResultPartitionID> releasablePartitions) {
		if (releasablePartitions.size() > 0) {
			final List<ResultPartitionID> partitionIds = releasablePartitions.stream()
				.map(this::createResultPartitionId)
				.collect(Collectors.toList());

			partitionTracker.stopTrackingAndReleasePartitions(partitionIds);
		}
	}

	ResultPartitionID createResultPartitionId(final IntermediateResultPartitionID resultPartitionId) {
		final SchedulingResultPartition<?, ?> schedulingResultPartition =
			getSchedulingTopology().getResultPartitionOrThrow(resultPartitionId);
		final SchedulingExecutionVertex<?, ?> producer = schedulingResultPartition.getProducer();
		final ExecutionVertexID producerId = producer.getId();
		final JobVertexID jobVertexId = producerId.getJobVertexId();
		final ExecutionJobVertex jobVertex = getJobVertex(jobVertexId);
		checkNotNull(jobVertex, "Unknown job vertex %s", jobVertexId);

		final ExecutionVertex[] taskVertices = jobVertex.getTaskVertices();
		final int subtaskIndex = producerId.getSubtaskIndex();
		checkState(subtaskIndex < taskVertices.length, "Invalid subtask index %d for job vertex %s", subtaskIndex, jobVertexId);

		final ExecutionVertex taskVertex = taskVertices[subtaskIndex];
		final Execution execution = taskVertex.getCurrentExecutionAttempt();
		return new ResultPartitionID(resultPartitionId, execution.getAttemptId());
	}

	/**
	 * Deserializes accumulators from a task state update.
	 *
	 * <p>This method never throws an exception!
	 *
	 * @param state The task execution state from which to deserialize the accumulators.
	 * @return The deserialized accumulators, of null, if there are no accumulators or an error occurred.
	 */
	private Map<String, Accumulator<?, ?>> deserializeAccumulators(TaskExecutionState state) {
		AccumulatorSnapshot serializedAccumulators = state.getAccumulators();

		if (serializedAccumulators != null) {
			try {
				return serializedAccumulators.deserializeUserAccumulators(userClassLoader);
			}
			catch (Throwable t) {
				// we catch Throwable here to include all form of linking errors that may
				// occur if user classes are missing in the classpath
				LOG.error("Failed to deserialize final accumulator results.", t);
			}
		}
		return null;
	}

	/**
	 * Schedule or updates consumers of the given result partition.
	 *
	 * @param partitionId specifying the result partition whose consumer shall be scheduled or updated
	 * @throws ExecutionGraphException if the schedule or update consumers operation could not be executed
	 */
	public void scheduleOrUpdateConsumers(ResultPartitionID partitionId) throws ExecutionGraphException {

		assertRunningInJobMasterMainThread();

		final Execution execution = currentExecutions.get(partitionId.getProducerId());

		if (execution == null) {
			throw new ExecutionGraphException("Cannot find execution for execution Id " +
				partitionId.getPartitionId() + '.');
		}
		else if (execution.getVertex() == null){
			throw new ExecutionGraphException("Execution with execution Id " +
				partitionId.getPartitionId() + " has no vertex assigned.");
		} else {
			execution.getVertex().scheduleOrUpdateConsumers(partitionId);
		}
	}

	public Map<ExecutionAttemptID, Execution> getRegisteredExecutions() {
		return Collections.unmodifiableMap(currentExecutions);
	}

	void registerExecution(Execution exec) {
		assertRunningInJobMasterMainThread();
		Execution previous = currentExecutions.putIfAbsent(exec.getAttemptId(), exec);
		if (previous != null) {
			failGlobal(new Exception("Trying to register execution " + exec + " for already used ID " + exec.getAttemptId()));
		}
	}

	void deregisterExecution(Execution exec) {
		assertRunningInJobMasterMainThread();
		Execution contained = currentExecutions.remove(exec.getAttemptId());

		if (contained != null && contained != exec) {
			failGlobal(new Exception("De-registering execution " + exec + " failed. Found for same ID execution " + contained));
		}
	}

	/**
	 * Updates the accumulators during the runtime of a job. Final accumulator results are transferred
	 * through the UpdateTaskExecutionState message.
	 * @param accumulatorSnapshot The serialized flink and user-defined accumulators
	 */
	public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
		Map<String, Accumulator<?, ?>> userAccumulators;
		try {
			userAccumulators = accumulatorSnapshot.deserializeUserAccumulators(userClassLoader);

			ExecutionAttemptID execID = accumulatorSnapshot.getExecutionAttemptID();
			Execution execution = currentExecutions.get(execID);
			if (execution != null) {
				execution.setAccumulators(userAccumulators);
			} else {
				LOG.debug("Received accumulator result for unknown execution {}.", execID);
			}
		} catch (Exception e) {
			LOG.error("Cannot update accumulators for job {}.", getJobID(), e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Listeners & Observers
	// --------------------------------------------------------------------------------------------

	public void registerJobStatusListener(JobStatusListener listener) {
		if (listener != null) {
			jobStatusListeners.add(listener);
		}
	}

	private void notifyJobStatusChange(JobStatus newState, Throwable error) {
		if (jobStatusListeners.size() > 0) {
			final long timestamp = System.currentTimeMillis();
			final Throwable serializedError = error == null ? null : new SerializedThrowable(error);

			for (JobStatusListener listener : jobStatusListeners) {
				try {
					listener.jobStatusChanges(getJobID(), newState, timestamp, serializedError);
				} catch (Throwable t) {
					LOG.warn("Error while notifying JobStatusListener", t);
				}
			}
		}
	}

	void notifyExecutionChange(
			final Execution execution,
			final ExecutionState newExecutionState,
			final Throwable error) {

		if (!isLegacyScheduling()) {
			return;
		}

		// see what this means for us. currently, the first FAILED state means -> FAILED
		if (newExecutionState == ExecutionState.FAILED) {
			final Throwable ex = error != null ? error : new FlinkException("Unknown Error (missing cause)");

			// by filtering out late failure calls, we can save some work in
			// avoiding redundant local failover
			if (execution.getGlobalModVersion() == globalModVersion) {
				try {
					// fail all checkpoints which the failed task has not yet acknowledged
					if (checkpointCoordinator != null) {
						checkpointCoordinator.failUnacknowledgedPendingCheckpointsFor(execution.getAttemptId(), ex);
					}

					failoverStrategy.onTaskFailure(execution, ex);
				}
				catch (Throwable t) {
					// bug in the failover strategy - fall back to global failover
					LOG.warn("Error in failover strategy - falling back to global restart", t);
					failGlobal(ex);
				}
			}
		}
	}

	void assertRunningInJobMasterMainThread() {
		if (!(jobMasterMainThreadExecutor instanceof ComponentMainThreadExecutor.DummyComponentMainThreadExecutor)) {
			jobMasterMainThreadExecutor.assertRunningInMainThread();
		}
	}

	void notifySchedulerNgAboutInternalTaskFailure(final ExecutionAttemptID attemptId, final Throwable t) {
		if (internalTaskFailuresListener != null) {
			internalTaskFailuresListener.notifyTaskFailure(attemptId, t);
		}
	}

	ShuffleMaster<?> getShuffleMaster() {
		return shuffleMaster;
	}

	public JobMasterPartitionTracker getPartitionTracker() {
		return partitionTracker;
	}

	public ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
		return resultPartitionAvailabilityChecker;
	}

	PartitionReleaseStrategy getPartitionReleaseStrategy() {
		return partitionReleaseStrategy;
	}
}

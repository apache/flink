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
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.OperatorCoordinatorCheckpointContext;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.FutureUtils.ConjunctFuture;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.entrypoint.ClusterEntryPointExceptionUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
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
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The execution graph is the central data structure that coordinates the distributed execution of a
 * data flow. It keeps representations of each parallel task, each intermediate stream, and the
 * communication between them.
 *
 * <p>The execution graph consists of the following constructs:
 *
 * <ul>
 *   <li>The {@link ExecutionJobVertex} represents one vertex from the JobGraph (usually one
 *       operation like "map" or "join") during execution. It holds the aggregated state of all
 *       parallel subtasks. The ExecutionJobVertex is identified inside the graph by the {@link
 *       JobVertexID}, which it takes from the JobGraph's corresponding JobVertex.
 *   <li>The {@link ExecutionVertex} represents one parallel subtask. For each ExecutionJobVertex,
 *       there are as many ExecutionVertices as the parallelism. The ExecutionVertex is identified
 *       by the ExecutionJobVertex and the index of the parallel subtask
 *   <li>The {@link Execution} is one attempt to execute a ExecutionVertex. There may be multiple
 *       Executions for the ExecutionVertex, in case of a failure, or in the case where some data
 *       needs to be recomputed because it is no longer available when requested by later
 *       operations. An Execution is always identified by an {@link ExecutionAttemptID}. All
 *       messages between the JobManager and the TaskManager about deployment of tasks and updates
 *       in the task status always use the ExecutionAttemptID to address the message receiver.
 * </ul>
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
    @Nonnull private ComponentMainThreadExecutor jobMasterMainThreadExecutor;

    /** {@code true} if all source tasks are stoppable. */
    private boolean isStoppable = true;

    /** All job vertices that are part of this graph. */
    private final Map<JobVertexID, ExecutionJobVertex> tasks;

    /** All vertices, in the order in which they were created. * */
    private final List<ExecutionJobVertex> verticesInCreationOrder;

    /** All intermediate results that are part of this graph. */
    private final Map<IntermediateDataSetID, IntermediateResult> intermediateResults;

    /** The currently executed tasks, for callbacks. */
    private final Map<ExecutionAttemptID, Execution> currentExecutions;

    /**
     * Listeners that receive messages when the entire job switches it status (such as from RUNNING
     * to FINISHED).
     */
    private final List<JobStatusListener> jobStatusListeners;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * execution graph transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final long[] stateTimestamps;

    /** The timeout for all messages that require a response/acknowledgement. */
    private final Time rpcTimeout;

    /** The timeout for slot allocations. */
    private final Time allocationTimeout;

    /** The classloader for the user code. Needed for calls into user code classes. */
    private final ClassLoader userClassLoader;

    /** Registered KvState instances reported by the TaskManagers. */
    private final KvStateLocationRegistry kvStateLocationRegistry;

    /** Blob writer used to offload RPC messages. */
    private final BlobWriter blobWriter;

    /** The total number of vertices currently in the execution graph. */
    private int numVerticesTotal;

    private final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory;

    private PartitionReleaseStrategy partitionReleaseStrategy;

    private DefaultExecutionTopology executionTopology;

    @Nullable private InternalFailuresListener internalTaskFailuresListener;

    /** Counts all restarts. Used by other Gauges/Meters and does not register to metric group. */
    private final Counter numberOfRestartsCounter = new SimpleCounter();

    // ------ Configuration of the Execution -------

    /**
     * The mode of scheduling. Decides how to select the initial set of tasks to be deployed. May
     * indicate to deploy all sources, or to deploy everything, or to deploy via backtracking from
     * results than need to be materialized.
     */
    private final ScheduleMode scheduleMode;

    /** The maximum number of prior execution attempts kept in history. */
    private final int maxPriorAttemptsHistoryLength;

    // ------ Execution status and progress. These values are volatile, and accessed under the lock
    // -------

    private int verticesFinished;

    /** Current status of the job execution. */
    private volatile JobStatus state = JobStatus.CREATED;

    /** A future that completes once the job has reached a terminal state. */
    private final CompletableFuture<JobStatus> terminationFuture = new CompletableFuture<>();

    /**
     * On each global recovery, this version is incremented. The version breaks conflicts between
     * concurrent restart attempts by local failover strategies.
     */
    private long globalModVersion;

    /**
     * The exception that caused the job to fail. This is set to the first root exception that was
     * not recoverable and triggered job failure.
     */
    private Throwable failureCause;

    /**
     * The extended failure cause information for the job. This exists in addition to
     * 'failureCause', to let 'failureCause' be a strong reference to the exception, while this info
     * holds no strong reference to any user-defined classes.
     */
    private ErrorInfo failureInfo;

    private final JobMasterPartitionTracker partitionTracker;

    private final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker;

    /** Future for an ongoing or completed scheduling action. */
    @Nullable private CompletableFuture<Void> schedulingFuture;

    // ------ Fields that are relevant to the execution and need to be cleared before archiving
    // -------

    /** The coordinator for checkpoints, if snapshot checkpoints are enabled. */
    @Nullable private CheckpointCoordinator checkpointCoordinator;

    /** TODO, replace it with main thread executor. */
    @Nullable private ScheduledExecutorService checkpointCoordinatorTimer;

    /**
     * Checkpoint stats tracker separate from the coordinator in order to be available after
     * archiving.
     */
    private CheckpointStatsTracker checkpointStatsTracker;

    // ------ Fields that are only relevant for archived execution graphs ------------
    @Nullable private String stateBackendName;

    private String jsonPlan;

    /** Shuffle master to register partitions for task deployment. */
    private final ShuffleMaster<?> shuffleMaster;

    private final ExecutionDeploymentListener executionDeploymentListener;
    private final ExecutionStateUpdateListener executionStateUpdateListener;

    // --------------------------------------------------------------------------------------------
    //   Constructors
    // --------------------------------------------------------------------------------------------

    public ExecutionGraph(
            JobInformation jobInformation,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            Time rpcTimeout,
            int maxPriorAttemptsHistoryLength,
            SlotProvider slotProvider,
            ClassLoader userClassLoader,
            BlobWriter blobWriter,
            Time allocationTimeout,
            PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            ScheduleMode scheduleMode,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp)
            throws IOException {

        this.jobInformation = Preconditions.checkNotNull(jobInformation);

        this.blobWriter = Preconditions.checkNotNull(blobWriter);

        this.scheduleMode = checkNotNull(scheduleMode);

        this.jobInformationOrBlobKey =
                BlobWriter.serializeAndTryOffload(
                        jobInformation, jobInformation.getJobId(), blobWriter);

        this.futureExecutor = Preconditions.checkNotNull(futureExecutor);
        this.ioExecutor = Preconditions.checkNotNull(ioExecutor);

        this.userClassLoader = Preconditions.checkNotNull(userClassLoader, "userClassLoader");

        this.tasks = new HashMap<>(16);
        this.intermediateResults = new HashMap<>(16);
        this.verticesInCreationOrder = new ArrayList<>(16);
        this.currentExecutions = new HashMap<>(16);

        this.jobStatusListeners = new ArrayList<>();

        this.stateTimestamps = new long[JobStatus.values().length];
        this.stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
        this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

        this.rpcTimeout = checkNotNull(rpcTimeout);
        this.allocationTimeout = checkNotNull(allocationTimeout);

        this.partitionReleaseStrategyFactory = checkNotNull(partitionReleaseStrategyFactory);

        this.kvStateLocationRegistry =
                new KvStateLocationRegistry(jobInformation.getJobId(), getAllVertices());

        this.globalModVersion = 1L;

        this.maxPriorAttemptsHistoryLength = maxPriorAttemptsHistoryLength;

        this.schedulingFuture = null;
        this.jobMasterMainThreadExecutor =
                new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
                        "ExecutionGraph is not initialized with proper main thread executor. "
                                + "Call to ExecutionGraph.start(...) required.");

        this.shuffleMaster = checkNotNull(shuffleMaster);

        this.partitionTracker = checkNotNull(partitionTracker);

        this.resultPartitionAvailabilityChecker =
                new ExecutionGraphResultPartitionAvailabilityChecker(
                        this::createResultPartitionId, partitionTracker);

        this.executionDeploymentListener = executionDeploymentListener;
        this.executionStateUpdateListener = executionStateUpdateListener;
    }

    public void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor) {
        this.jobMasterMainThreadExecutor = jobMasterMainThreadExecutor;
    }

    // --------------------------------------------------------------------------------------------
    //  Configuration of Data-flow wide execution settings
    // --------------------------------------------------------------------------------------------

    /**
     * Gets the number of job vertices currently held by this execution graph.
     *
     * @return The current number of job vertices.
     */
    public int getNumberOfExecutionJobVertices() {
        return this.verticesInCreationOrder.size();
    }

    public SchedulingTopology getSchedulingTopology() {
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
            CheckpointStatsTracker statsTracker,
            CheckpointsCleaner checkpointsCleaner) {

        checkState(state == JobStatus.CREATED, "Job must be in CREATED state");
        checkState(checkpointCoordinator == null, "checkpointing already enabled");

        ExecutionVertex[] tasksToTrigger = collectExecutionVertices(verticesToTrigger);
        ExecutionVertex[] tasksToWaitFor = collectExecutionVertices(verticesToWaitFor);
        ExecutionVertex[] tasksToCommitTo = collectExecutionVertices(verticesToCommitTo);

        final Collection<OperatorCoordinatorCheckpointContext> operatorCoordinators =
                buildOpCoordinatorCheckpointContexts();

        checkpointStatsTracker = checkNotNull(statsTracker, "CheckpointStatsTracker");

        CheckpointFailureManager failureManager =
                new CheckpointFailureManager(
                        chkConfig.getTolerableCheckpointFailureNumber(),
                        new CheckpointFailureManager.FailJobCallback() {
                            @Override
                            public void failJob(Throwable cause) {
                                getJobMasterMainThreadExecutor().execute(() -> failGlobal(cause));
                            }

                            @Override
                            public void failJobDueToTaskFailure(
                                    Throwable cause, ExecutionAttemptID failingTask) {
                                getJobMasterMainThreadExecutor()
                                        .execute(
                                                () ->
                                                        failGlobalIfExecutionIsStillRunning(
                                                                cause, failingTask));
                            }
                        });

        checkState(checkpointCoordinatorTimer == null);

        checkpointCoordinatorTimer =
                Executors.newSingleThreadScheduledExecutor(
                        new DispatcherThreadFactory(
                                Thread.currentThread().getThreadGroup(), "Checkpoint Timer"));

        // create the coordinator that triggers and commits checkpoints and holds the state
        checkpointCoordinator =
                new CheckpointCoordinator(
                        jobInformation.getJobId(),
                        chkConfig,
                        tasksToTrigger,
                        tasksToWaitFor,
                        tasksToCommitTo,
                        operatorCoordinators,
                        checkpointIDCounter,
                        checkpointStore,
                        checkpointStateBackend,
                        ioExecutor,
                        checkpointsCleaner,
                        new ScheduledExecutorServiceAdapter(checkpointCoordinatorTimer),
                        SharedStateRegistry.DEFAULT_FACTORY,
                        failureManager);

        // register the master hooks on the checkpoint coordinator
        for (MasterTriggerRestoreHook<?> hook : masterHooks) {
            if (!checkpointCoordinator.addMasterHook(hook)) {
                LOG.warn(
                        "Trying to register multiple checkpoint hooks with the name: {}",
                        hook.getIdentifier());
            }
        }

        checkpointCoordinator.setCheckpointStatsTracker(checkpointStatsTracker);

        // interval of max long value indicates disable periodic checkpoint,
        // the CheckpointActivatorDeactivator should be created only if the interval is not max
        // value
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
                throw new IllegalArgumentException(
                        "Can only use ExecutionJobVertices of this ExecutionGraph");
            }
            return jv.getTaskVertices();
        } else {
            ArrayList<ExecutionVertex> all = new ArrayList<>();
            for (ExecutionJobVertex jv : jobVertices) {
                if (jv.getGraph() != this) {
                    throw new IllegalArgumentException(
                            "Can only use ExecutionJobVertices of this ExecutionGraph");
                }
                all.addAll(Arrays.asList(jv.getTaskVertices()));
            }
            return all.toArray(new ExecutionVertex[all.size()]);
        }
    }

    private Collection<OperatorCoordinatorCheckpointContext>
            buildOpCoordinatorCheckpointContexts() {
        final ArrayList<OperatorCoordinatorCheckpointContext> contexts = new ArrayList<>();
        for (final ExecutionJobVertex vertex : verticesInCreationOrder) {
            contexts.addAll(vertex.getOperatorCoordinators());
        }
        contexts.trimToSize();
        return contexts;
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
     * Gets the number of restarts, including full restarts and fine grained restarts. If a recovery
     * is currently pending, this recovery is included in the count.
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
     *
     * @return The accumulator map
     */
    public Map<String, OptionalFailure<Accumulator<?, ?>>> aggregateUserAccumulators() {

        Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = new HashMap<>();

        for (ExecutionVertex vertex : getAllExecutionVertices()) {
            Map<String, Accumulator<?, ?>> next =
                    vertex.getCurrentExecutionAttempt().getUserAccumulators();
            if (next != null) {
                AccumulatorHelper.mergeInto(userAccumulators, next);
            }
        }

        return userAccumulators;
    }

    /**
     * Gets a serialized accumulator map.
     *
     * @return The accumulator map with serialized accumulator values.
     */
    @Override
    public Map<String, SerializedValue<OptionalFailure<Object>>> getAccumulatorsSerialized() {
        return aggregateUserAccumulators().entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> serializeAccumulator(entry.getKey(), entry.getValue())));
    }

    private static SerializedValue<OptionalFailure<Object>> serializeAccumulator(
            String name, OptionalFailure<Accumulator<?, ?>> accumulator) {
        try {
            if (accumulator.isFailure()) {
                return new SerializedValue<>(
                        OptionalFailure.ofFailure(accumulator.getFailureCause()));
            }
            return new SerializedValue<>(
                    OptionalFailure.of(accumulator.getUnchecked().getLocalValue()));
        } catch (IOException ioe) {
            LOG.error("Could not serialize accumulator " + name + '.', ioe);
            try {
                return new SerializedValue<>(OptionalFailure.ofFailure(ioe));
            } catch (IOException e) {
                throw new RuntimeException(
                        "It should never happen that we cannot serialize the accumulator serialization exception.",
                        e);
            }
        }
    }

    /**
     * Returns the a stringified version of the user-defined accumulators.
     *
     * @return an Array containing the StringifiedAccumulatorResult objects
     */
    @Override
    public StringifiedAccumulatorResult[] getAccumulatorResultsStringified() {
        Map<String, OptionalFailure<Accumulator<?, ?>>> accumulatorMap =
                aggregateUserAccumulators();
        return StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);
    }

    public void setInternalTaskFailuresListener(
            final InternalFailuresListener internalTaskFailuresListener) {
        checkNotNull(internalTaskFailuresListener);
        checkState(
                this.internalTaskFailuresListener == null,
                "internalTaskFailuresListener can be only set once");
        this.internalTaskFailuresListener = internalTaskFailuresListener;
    }

    // --------------------------------------------------------------------------------------------
    //  Actions
    // --------------------------------------------------------------------------------------------

    public void attachJobGraph(List<JobVertex> topologiallySorted) throws JobException {

        assertRunningInJobMasterMainThread();

        LOG.debug(
                "Attaching {} topologically sorted vertices to existing job graph with {} "
                        + "vertices and {} intermediate results.",
                topologiallySorted.size(),
                tasks.size(),
                intermediateResults.size());

        final ArrayList<ExecutionJobVertex> newExecJobVertices =
                new ArrayList<>(topologiallySorted.size());
        final long createTimestamp = System.currentTimeMillis();

        for (JobVertex jobVertex : topologiallySorted) {

            if (jobVertex.isInputVertex() && !jobVertex.isStoppable()) {
                this.isStoppable = false;
            }

            // create the execution job vertex and attach it to the graph
            ExecutionJobVertex ejv =
                    new ExecutionJobVertex(
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
                throw new JobException(
                        String.format(
                                "Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
                                jobVertex.getID(), ejv, previousTask));
            }

            for (IntermediateResult res : ejv.getProducedDataSets()) {
                IntermediateResult previousDataSet =
                        this.intermediateResults.putIfAbsent(res.getId(), res);
                if (previousDataSet != null) {
                    throw new JobException(
                            String.format(
                                    "Encountered two intermediate data set with ID %s : previous=[%s] / new=[%s]",
                                    res.getId(), res, previousDataSet));
                }
            }

            this.verticesInCreationOrder.add(ejv);
            this.numVerticesTotal += ejv.getParallelism();
            newExecJobVertices.add(ejv);
        }

        // the topology assigning should happen before notifying new vertices to failoverStrategy
        executionTopology = DefaultExecutionTopology.fromExecutionGraph(this);

        partitionReleaseStrategy =
                partitionReleaseStrategyFactory.createInstance(getSchedulingTopology());
    }

    public void transitionToRunning() {
        if (!transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
            throw new IllegalStateException(
                    "Job may only be scheduled from state " + JobStatus.CREATED);
        }
    }

    public void cancel() {

        assertRunningInJobMasterMainThread();

        while (true) {
            JobStatus current = state;

            if (current == JobStatus.RUNNING
                    || current == JobStatus.CREATED
                    || current == JobStatus.RESTARTING) {
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
                                                    "Could not cancel job "
                                                            + getJobName()
                                                            + " because not all execution job vertices could be cancelled.",
                                                    throwable));
                                } else {
                                    // cancellations may currently be overridden by failures which
                                    // trigger
                                    // restarts, so we need to pass a proper restart global version
                                    // here
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
            } else {
                // no need to treat other states
                return;
            }
        }
    }

    private ConjunctFuture<Void> cancelVerticesAsync() {
        final ArrayList<CompletableFuture<?>> futures =
                new ArrayList<>(verticesInCreationOrder.size());

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
     * <p>The JobStatus will be directly set to {@link JobStatus#SUSPENDED} iff the current state is
     * not a terminal state. All ExecutionJobVertices will be canceled and the onTerminalState() is
     * executed.
     *
     * <p>The {@link JobStatus#SUSPENDED} state is a local terminal state which stops the execution
     * of the job but does not remove the job from the HA job store so that it can be recovered by
     * another JobManager.
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
            final ArrayList<CompletableFuture<Void>> executionJobVertexTerminationFutures =
                    new ArrayList<>(verticesInCreationOrder.size());

            for (ExecutionJobVertex ejv : verticesInCreationOrder) {
                executionJobVertexTerminationFutures.add(ejv.suspend());
            }

            final ConjunctFuture<Void> jobVerticesTerminationFuture =
                    FutureUtils.waitForAll(executionJobVertexTerminationFutures);

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
            throw new IllegalStateException(
                    String.format(
                            "Could not suspend because transition from %s to %s failed.",
                            state, JobStatus.SUSPENDED));
        }
    }

    void failGlobalIfExecutionIsStillRunning(Throwable cause, ExecutionAttemptID failingAttempt) {
        final Execution failedExecution = currentExecutions.get(failingAttempt);
        if (failedExecution != null && failedExecution.getState() == ExecutionState.RUNNING) {
            failGlobal(cause);
        } else {
            LOG.debug(
                    "The failing attempt {} belongs to an already not"
                            + " running task thus won't fail the job",
                    failingAttempt);
        }
    }

    /**
     * Fails the execution graph globally.
     *
     * <p>This global failure is meant to be triggered in cases where the consistency of the
     * execution graph' state cannot be guaranteed any more (for example when catching unexpected
     * exceptions that indicate a bug or an unexpected call race), and where a full restart is the
     * safe way to get consistency back.
     *
     * @param t The exception that caused the failure.
     */
    public void failGlobal(Throwable t) {
        checkState(internalTaskFailuresListener != null);
        internalTaskFailuresListener.notifyGlobalFailure(t);
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
            ExecutionConfig executionConfig =
                    jobInformation.getSerializedExecutionConfig().deserializeValue(userClassLoader);
            if (executionConfig != null) {
                return executionConfig.archive();
            }
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("Couldn't create ArchivedExecutionConfig for job {} ", getJobID(), e);
        }
        return null;
    }

    /**
     * Returns the termination future of this {@link ExecutionGraph}. The termination future is
     * completed with the terminal {@link JobStatus} once the ExecutionGraph reaches this terminal
     * state and all {@link Execution} have been terminated.
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
        } catch (ExecutionException e) {
            // this should never happen
            // it would be a bug, so we  don't expect this to be handled and throw
            // an unchecked exception here
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the current global modification version of the ExecutionGraph. The global modification
     * version is incremented with each global action (cancel/fail/restart) and is used to
     * disambiguate concurrent modifications between local and global failover actions.
     */
    public long getGlobalModVersion() {
        return globalModVersion;
    }

    // ------------------------------------------------------------------------
    //  State Transitions
    // ------------------------------------------------------------------------

    public boolean transitionState(JobStatus current, JobStatus newState) {
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
            LOG.info(
                    "Job {} ({}) switched from state {} to {}.",
                    getJobName(),
                    getJobID(),
                    current,
                    newState,
                    error);

            stateTimestamps[newState.ordinal()] = System.currentTimeMillis();
            notifyJobStatusChange(newState, error);
            return true;
        } else {
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
     * Called whenever a vertex reaches state FINISHED (completed successfully). Once all vertices
     * are in the FINISHED state, the program is successfully done.
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
                } catch (Throwable t) {
                    ExceptionUtils.rethrowIfFatalError(t);
                    ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(t);
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
     * This method is a callback during cancellation/failover and called when all tasks have reached
     * a terminal state (cancelled/failed/finished).
     */
    private void allVerticesInTerminalState(long expectedGlobalVersionForRestart) {

        assertRunningInJobMasterMainThread();

        // we are done, transition to the final state
        JobStatus current;
        while (true) {
            current = this.state;

            if (current == JobStatus.RUNNING) {
                failGlobal(
                        new Exception(
                                "ExecutionGraph went into allVerticesInTerminalState() from RUNNING"));
            } else if (current == JobStatus.CANCELLING) {
                if (transitionState(current, JobStatus.CANCELED)) {
                    onTerminalState(JobStatus.CANCELED);
                    break;
                }
            } else if (current == JobStatus.FAILING) {
                break;
            } else if (current.isGloballyTerminalState()) {
                LOG.warn(
                        "Job has entered globally terminal state without waiting for all "
                                + "job vertices to reach final state.");
                break;
            } else {
                failGlobal(
                        new Exception(
                                "ExecutionGraph went into final state from state " + current));
                break;
            }
        }
        // done transitioning the state
    }

    public void failJob(Throwable cause) {
        if (state == JobStatus.FAILING || state.isTerminalState()) {
            return;
        }

        transitionState(JobStatus.FAILING, cause);
        initFailureCause(cause);

        FutureUtils.assertNoException(
                cancelVerticesAsync()
                        .whenComplete(
                                (aVoid, throwable) -> {
                                    if (transitionState(
                                            JobStatus.FAILING, JobStatus.FAILED, cause)) {
                                        onTerminalState(JobStatus.FAILED);
                                    } else if (state == JobStatus.CANCELLING) {
                                        transitionState(JobStatus.CANCELLING, JobStatus.CANCELED);
                                        onTerminalState(JobStatus.CANCELED);
                                    } else if (!state.isTerminalState()) {
                                        throw new IllegalStateException(
                                                "Cannot complete job failing from an unexpected state: "
                                                        + state);
                                    }
                                }));
    }

    private void onTerminalState(JobStatus status) {
        try {
            CheckpointCoordinator coord = this.checkpointCoordinator;
            this.checkpointCoordinator = null;
            if (coord != null) {
                coord.shutdown();
            }
            if (checkpointCoordinatorTimer != null) {
                checkpointCoordinatorTimer.shutdownNow();
                checkpointCoordinatorTimer = null;
            }
        } catch (Exception e) {
            LOG.error("Error while cleaning up after execution", e);
        } finally {
            terminationFuture.complete(status);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Callbacks and Callback Utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Updates the state of one of the ExecutionVertex's Execution attempts. If the new status if
     * "FINISHED", this also updates the accumulators.
     *
     * @param state The state update.
     * @return True, if the task update was properly applied, false, if the execution attempt was
     *     not found.
     */
    public boolean updateState(TaskExecutionStateTransition state) {
        assertRunningInJobMasterMainThread();
        final Execution attempt = currentExecutions.get(state.getID());

        if (attempt != null) {
            try {
                final boolean stateUpdated = updateStateInternal(state, attempt);
                maybeReleasePartitions(attempt);
                return stateUpdated;
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                // failures during updates leave the ExecutionGraph inconsistent
                failGlobal(t);
                return false;
            }
        } else {
            return false;
        }
    }

    private boolean updateStateInternal(
            final TaskExecutionStateTransition state, final Execution attempt) {
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
                attempt.markFailed(
                        state.getError(userClassLoader),
                        state.getCancelTask(),
                        accumulators,
                        state.getIOMetrics(),
                        state.getReleasePartitions(),
                        true);
                return true;

            default:
                // we mark as failed and return false, which triggers the TaskManager
                // to remove the task
                attempt.fail(
                        new Exception(
                                "TaskManager sent illegal state update: "
                                        + state.getExecutionState()));
                return false;
        }
    }

    private void maybeReleasePartitions(final Execution attempt) {
        final ExecutionVertexID finishedExecutionVertex = attempt.getVertex().getID();

        if (attempt.getState() == ExecutionState.FINISHED) {
            final List<IntermediateResultPartitionID> releasablePartitions =
                    partitionReleaseStrategy.vertexFinished(finishedExecutionVertex);
            releasePartitions(releasablePartitions);
        } else {
            partitionReleaseStrategy.vertexUnfinished(finishedExecutionVertex);
        }
    }

    private void releasePartitions(final List<IntermediateResultPartitionID> releasablePartitions) {
        if (releasablePartitions.size() > 0) {
            final List<ResultPartitionID> partitionIds =
                    releasablePartitions.stream()
                            .map(this::createResultPartitionId)
                            .collect(Collectors.toList());

            partitionTracker.stopTrackingAndReleasePartitions(partitionIds);
        }
    }

    ResultPartitionID createResultPartitionId(
            final IntermediateResultPartitionID resultPartitionId) {
        final SchedulingResultPartition schedulingResultPartition =
                getSchedulingTopology().getResultPartition(resultPartitionId);
        final SchedulingExecutionVertex producer = schedulingResultPartition.getProducer();
        final ExecutionVertexID producerId = producer.getId();
        final JobVertexID jobVertexId = producerId.getJobVertexId();
        final ExecutionJobVertex jobVertex = getJobVertex(jobVertexId);
        checkNotNull(jobVertex, "Unknown job vertex %s", jobVertexId);

        final ExecutionVertex[] taskVertices = jobVertex.getTaskVertices();
        final int subtaskIndex = producerId.getSubtaskIndex();
        checkState(
                subtaskIndex < taskVertices.length,
                "Invalid subtask index %d for job vertex %s",
                subtaskIndex,
                jobVertexId);

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
     * @return The deserialized accumulators, of null, if there are no accumulators or an error
     *     occurred.
     */
    private Map<String, Accumulator<?, ?>> deserializeAccumulators(
            TaskExecutionStateTransition state) {
        AccumulatorSnapshot serializedAccumulators = state.getAccumulators();

        if (serializedAccumulators != null) {
            try {
                return serializedAccumulators.deserializeUserAccumulators(userClassLoader);
            } catch (Throwable t) {
                // we catch Throwable here to include all form of linking errors that may
                // occur if user classes are missing in the classpath
                LOG.error("Failed to deserialize final accumulator results.", t);
            }
        }
        return null;
    }

    /**
     * Mark the data of a result partition to be available. Note that only PIPELINED partitions are
     * accepted because it is for the case that a TM side PIPELINED result partition has data
     * produced and notifies JM.
     *
     * @param partitionId specifying the result partition whose data have become available
     */
    public void notifyPartitionDataAvailable(ResultPartitionID partitionId) {
        assertRunningInJobMasterMainThread();

        final Execution execution = currentExecutions.get(partitionId.getProducerId());

        checkState(
                execution != null,
                "Cannot find execution for execution Id " + partitionId.getPartitionId() + ".");

        execution.getVertex().notifyPartitionDataAvailable(partitionId);
    }

    public Map<ExecutionAttemptID, Execution> getRegisteredExecutions() {
        return Collections.unmodifiableMap(currentExecutions);
    }

    void registerExecution(Execution exec) {
        assertRunningInJobMasterMainThread();
        Execution previous = currentExecutions.putIfAbsent(exec.getAttemptId(), exec);
        if (previous != null) {
            failGlobal(
                    new Exception(
                            "Trying to register execution "
                                    + exec
                                    + " for already used ID "
                                    + exec.getAttemptId()));
        }
    }

    void deregisterExecution(Execution exec) {
        assertRunningInJobMasterMainThread();
        Execution contained = currentExecutions.remove(exec.getAttemptId());

        if (contained != null && contained != exec) {
            failGlobal(
                    new Exception(
                            "De-registering execution "
                                    + exec
                                    + " failed. Found for same ID execution "
                                    + contained));
        }
    }

    /**
     * Updates the accumulators during the runtime of a job. Final accumulator results are
     * transferred through the UpdateTaskExecutionState message.
     *
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

    void notifyExecutionChange(final Execution execution, final ExecutionState newExecutionState) {
        executionStateUpdateListener.onStateUpdate(execution.getAttemptId(), newExecutionState);
    }

    void assertRunningInJobMasterMainThread() {
        if (!(jobMasterMainThreadExecutor
                instanceof ComponentMainThreadExecutor.DummyComponentMainThreadExecutor)) {
            jobMasterMainThreadExecutor.assertRunningInMainThread();
        }
    }

    void notifySchedulerNgAboutInternalTaskFailure(
            final ExecutionAttemptID attemptId,
            final Throwable t,
            final boolean cancelTask,
            final boolean releasePartitions) {
        checkState(internalTaskFailuresListener != null);
        internalTaskFailuresListener.notifyTaskFailure(attemptId, t, cancelTask, releasePartitions);
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

    ExecutionDeploymentListener getExecutionDeploymentListener() {
        return executionDeploymentListener;
    }
}

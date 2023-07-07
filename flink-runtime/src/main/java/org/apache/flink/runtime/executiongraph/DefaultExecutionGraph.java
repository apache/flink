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
import org.apache.flink.core.execution.JobStatusHook;
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
import org.apache.flink.runtime.checkpoint.DefaultCheckpointPlanCalculator;
import org.apache.flink.runtime.checkpoint.DefaultFlushPlanCalculator;
import org.apache.flink.runtime.checkpoint.PlanCalculator;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.OperatorCoordinatorCheckpointContext;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntryPointExceptionUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertex.FinalizeOnMasterContext;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.SsgNetworkMemoryCalculationUtils;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionTopology;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.FutureUtils.ConjunctFuture;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

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

/** Default implementation of the {@link ExecutionGraph}. */
public class DefaultExecutionGraph implements ExecutionGraph, InternalExecutionGraphAccessor {

    /** The log object used for debugging. */
    static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

    // --------------------------------------------------------------------------------------------

    /**
     * The unique id of an execution graph. It is different from JobID, because there can be
     * multiple execution graphs created from one job graph, in cases like job re-submission, job
     * master failover and job rescaling.
     */
    private final ExecutionGraphID executionGraphId;

    /** Job specific information like the job id, job name, job configuration, etc. */
    private final JobInformation jobInformation;

    /** The executor which is used to execute futures. */
    private final ScheduledExecutorService futureExecutor;

    /** The executor which is used to execute blocking io operations. */
    private final Executor ioExecutor;

    /** {@link CoordinatorStore} shared across all operator coordinators within this execution. */
    private final CoordinatorStore coordinatorStore = new CoordinatorStoreImpl();

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

    /** The classloader for the user code. Needed for calls into user code classes. */
    private final ClassLoader userClassLoader;

    /** Registered KvState instances reported by the TaskManagers. */
    private final KvStateLocationRegistry kvStateLocationRegistry;

    /** Blob writer used to offload RPC messages. */
    private final BlobWriter blobWriter;

    /** Number of total job vertices. */
    private int numJobVerticesTotal;

    private final PartitionGroupReleaseStrategy.Factory partitionGroupReleaseStrategyFactory;

    private PartitionGroupReleaseStrategy partitionGroupReleaseStrategy;

    private DefaultExecutionTopology executionTopology;

    @Nullable private InternalFailuresListener internalTaskFailuresListener;

    /** Counts all restarts. Used by other Gauges/Meters and does not register to metric group. */
    private final Counter numberOfRestartsCounter = new SimpleCounter();

    // ------ Configuration of the Execution -------

    /** The maximum number of historical execution attempts kept in history. */
    private final int executionHistorySizeLimit;

    // ------ Execution status and progress. These values are volatile, and accessed under the lock
    // -------

    /** Number of finished job vertices. */
    private int numFinishedJobVertices;

    /** Current status of the job execution. */
    private volatile JobStatus state = JobStatus.CREATED;

    /** A future that completes once the job has reached a terminal state. */
    private final CompletableFuture<JobStatus> terminationFuture = new CompletableFuture<>();

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

    private final VertexAttemptNumberStore initialAttemptCounts;

    private final VertexParallelismStore parallelismStore;

    // ------ Fields that are relevant to the execution and need to be cleared before archiving
    // -------

    @Nullable private CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration;

    /** The coordinator for checkpoints, if snapshot checkpoints are enabled. */
    @Nullable private CheckpointCoordinator checkpointCoordinator;

    /** TODO, replace it with main thread executor. */
    @Nullable private ScheduledExecutorService checkpointCoordinatorTimer;

    @Nullable private ScheduledExecutorService flushEventTimer;

    /**
     * Checkpoint stats tracker separate from the coordinator in order to be available after
     * archiving.
     */
    private CheckpointStatsTracker checkpointStatsTracker;

    // ------ Fields that are only relevant for archived execution graphs ------------
    @Nullable private String stateBackendName;

    @Nullable private String checkpointStorageName;

    @Nullable private String changelogStorageName;

    @Nullable private TernaryBoolean stateChangelogEnabled;

    private String jsonPlan;

    /** Shuffle master to register partitions for task deployment. */
    private final ShuffleMaster<?> shuffleMaster;

    private final ExecutionDeploymentListener executionDeploymentListener;
    private final ExecutionStateUpdateListener executionStateUpdateListener;

    private final EdgeManager edgeManager;

    private final Map<ExecutionVertexID, ExecutionVertex> executionVerticesById;
    private final Map<IntermediateResultPartitionID, IntermediateResultPartition>
            resultPartitionsById;

    private final VertexInputInfoStore vertexInputInfoStore;
    private final boolean isDynamic;

    private final ExecutionJobVertex.Factory executionJobVertexFactory;

    private final List<JobStatusHook> jobStatusHooks;

    private final MarkPartitionFinishedStrategy markPartitionFinishedStrategy;

    private final TaskDeploymentDescriptorFactory taskDeploymentDescriptorFactory;

    // --------------------------------------------------------------------------------------------
    //   Constructors
    // --------------------------------------------------------------------------------------------

    public DefaultExecutionGraph(
            JobInformation jobInformation,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            Time rpcTimeout,
            int executionHistorySizeLimit,
            ClassLoader userClassLoader,
            BlobWriter blobWriter,
            PartitionGroupReleaseStrategy.Factory partitionGroupReleaseStrategyFactory,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp,
            VertexAttemptNumberStore initialAttemptCounts,
            VertexParallelismStore vertexParallelismStore,
            boolean isDynamic,
            ExecutionJobVertex.Factory executionJobVertexFactory,
            List<JobStatusHook> jobStatusHooks,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            TaskDeploymentDescriptorFactory taskDeploymentDescriptorFactory) {

        this.executionGraphId = new ExecutionGraphID();

        this.jobInformation = checkNotNull(jobInformation);

        this.blobWriter = checkNotNull(blobWriter);

        this.futureExecutor = checkNotNull(futureExecutor);
        this.ioExecutor = checkNotNull(ioExecutor);

        this.userClassLoader = checkNotNull(userClassLoader, "userClassLoader");

        this.tasks = CollectionUtil.newHashMapWithExpectedSize(16);
        this.intermediateResults = CollectionUtil.newHashMapWithExpectedSize(16);
        this.verticesInCreationOrder = new ArrayList<>(16);
        this.currentExecutions = CollectionUtil.newHashMapWithExpectedSize(16);

        this.jobStatusListeners = new ArrayList<>();

        this.stateTimestamps = new long[JobStatus.values().length];
        this.stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
        this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

        this.rpcTimeout = checkNotNull(rpcTimeout);

        this.partitionGroupReleaseStrategyFactory =
                checkNotNull(partitionGroupReleaseStrategyFactory);

        this.kvStateLocationRegistry =
                new KvStateLocationRegistry(jobInformation.getJobId(), getAllVertices());

        this.executionHistorySizeLimit = executionHistorySizeLimit;

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

        this.initialAttemptCounts = initialAttemptCounts;

        this.parallelismStore = vertexParallelismStore;

        this.edgeManager = new EdgeManager();
        this.executionVerticesById = new HashMap<>();
        this.resultPartitionsById = new HashMap<>();
        this.vertexInputInfoStore = new VertexInputInfoStore();

        this.isDynamic = isDynamic;

        this.executionJobVertexFactory = checkNotNull(executionJobVertexFactory);

        this.jobStatusHooks = checkNotNull(jobStatusHooks);

        this.markPartitionFinishedStrategy = markPartitionFinishedStrategy;

        this.taskDeploymentDescriptorFactory = checkNotNull(taskDeploymentDescriptorFactory);

        LOG.info(
                "Created execution graph {} for job {}.",
                executionGraphId,
                jobInformation.getJobId());
        // Trigger hook onCreated
        notifyJobStatusHooks(state, null);
    }

    @Override
    public void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor) {
        this.jobMasterMainThreadExecutor = jobMasterMainThreadExecutor;
    }

    // --------------------------------------------------------------------------------------------
    //  Configuration of Data-flow wide execution settings
    // --------------------------------------------------------------------------------------------

    @Override
    public SchedulingTopology getSchedulingTopology() {
        return executionTopology;
    }

    @Override
    @Nonnull
    public ComponentMainThreadExecutor getJobMasterMainThreadExecutor() {
        return jobMasterMainThreadExecutor;
    }

    @Override
    public TernaryBoolean isChangelogStateBackendEnabled() {
        return stateChangelogEnabled;
    }

    @Override
    public Optional<String> getStateBackendName() {
        return Optional.ofNullable(stateBackendName);
    }

    @Override
    public Optional<String> getCheckpointStorageName() {
        return Optional.ofNullable(checkpointStorageName);
    }

    @Override
    public Optional<String> getChangelogStorageName() {
        return Optional.ofNullable(changelogStorageName);
    }

    @Override
    public void enableCheckpointing(
            CheckpointCoordinatorConfiguration chkConfig,
            List<MasterTriggerRestoreHook<?>> masterHooks,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore checkpointStore,
            StateBackend checkpointStateBackend,
            CheckpointStorage checkpointStorage,
            CheckpointStatsTracker statsTracker,
            CheckpointsCleaner checkpointsCleaner,
            String changelogStorageName) {

        checkState(state == JobStatus.CREATED, "Job must be in CREATED state");
        checkState(checkpointCoordinator == null, "checkpointing already enabled");

        final Collection<OperatorCoordinatorCheckpointContext> operatorCoordinators =
                buildOpCoordinatorCheckpointContexts();

        checkpointStatsTracker = checkNotNull(statsTracker, "CheckpointStatsTracker");
        checkpointCoordinatorConfiguration =
                checkNotNull(chkConfig, "CheckpointCoordinatorConfiguration");

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
        checkState(flushEventTimer == null);

        checkpointCoordinatorTimer =
                Executors.newSingleThreadScheduledExecutor(
                        new DispatcherThreadFactory(
                                Thread.currentThread().getThreadGroup(), "Checkpoint Timer"));
        flushEventTimer = Executors.newSingleThreadScheduledExecutor(
                new DispatcherThreadFactory(
                        Thread.currentThread().getThreadGroup(), "Flush Event Timer"));

        // create the coordinator that triggers and commits checkpoints and holds the state
        checkpointCoordinator =
                new CheckpointCoordinator(
                        jobInformation.getJobId(),
                        chkConfig,
                        operatorCoordinators,
                        checkpointIDCounter,
                        checkpointStore,
                        checkpointStorage,
                        ioExecutor,
                        checkpointsCleaner,
                        new ScheduledExecutorServiceAdapter(checkpointCoordinatorTimer),
                        failureManager,
                        createCheckpointPlanCalculator(
                                chkConfig.isEnableCheckpointsAfterTasksFinish()),
                        checkpointStatsTracker,
                        new ScheduledExecutorServiceAdapter(flushEventTimer),
                        createFlushPlanCalculator());

        // register the master hooks on the checkpoint coordinator
        for (MasterTriggerRestoreHook<?> hook : masterHooks) {
            if (!checkpointCoordinator.addMasterHook(hook)) {
                LOG.warn(
                        "Trying to register multiple checkpoint hooks with the name: {}",
                        hook.getIdentifier());
            }
        }

        if (checkpointCoordinator.isPeriodicCheckpointingConfigured() || checkpointCoordinator.isAllowedLatencyConfigured()) {
            // the periodic checkpoint scheduler is activated and deactivated as a result of
            // job status changes (running -> on, all other states -> off)
            registerJobStatusListener(checkpointCoordinator.createActivatorDeactivator());
        }

        this.stateBackendName = checkpointStateBackend.getName();
        this.stateChangelogEnabled =
                TernaryBoolean.fromBoolean(
                        StateBackendLoader.isChangelogStateBackend(checkpointStateBackend));

        this.checkpointStorageName = checkpointStorage.getClass().getSimpleName();
        this.changelogStorageName = changelogStorageName;
    }

    private PlanCalculator createCheckpointPlanCalculator(
            boolean enableCheckpointsAfterTasksFinish) {
        return new DefaultCheckpointPlanCalculator(
                getJobID(),
                new ExecutionGraphCheckpointPlanCalculatorContext(this),
                getVerticesTopologically(),
                enableCheckpointsAfterTasksFinish);
    }

    private PlanCalculator createFlushPlanCalculator() {
        return new DefaultFlushPlanCalculator(
                getJobID(),
                new ExecutionGraphCheckpointPlanCalculatorContext(this),
                getVerticesTopologically());
    }

    @Override
    @Nullable
    public CheckpointCoordinator getCheckpointCoordinator() {
        return checkpointCoordinator;
    }

    @Override
    public KvStateLocationRegistry getKvStateLocationRegistry() {
        return kvStateLocationRegistry;
    }

    @Override
    public CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration() {
        if (checkpointCoordinatorConfiguration != null) {
            return checkpointCoordinatorConfiguration;
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

    @Override
    public void setJsonPlan(String jsonPlan) {
        this.jsonPlan = jsonPlan;
    }

    @Override
    public String getJsonPlan() {
        return jsonPlan;
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

    @Override
    public Configuration getJobConfiguration() {
        return jobInformation.getJobConfiguration();
    }

    @Override
    public ClassLoader getUserClassLoader() {
        return this.userClassLoader;
    }

    @Override
    public JobStatus getState() {
        return state;
    }

    @Override
    public Throwable getFailureCause() {
        return failureCause;
    }

    public ErrorInfo getFailureInfo() {
        return failureInfo;
    }

    @Override
    public long getNumberOfRestarts() {
        return numberOfRestartsCounter.getCount();
    }

    @Override
    public int getNumFinishedVertices() {
        return IterableUtils.toStream(getVerticesTopologically())
                .map(ExecutionJobVertex::getNumExecutionVertexFinished)
                .mapToInt(Integer::intValue)
                .sum();
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

    @Override
    public Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults() {
        return Collections.unmodifiableMap(this.intermediateResults);
    }

    @Override
    public Iterable<ExecutionVertex> getAllExecutionVertices() {
        return () -> new AllVerticesIterator<>(getVerticesTopologically().iterator());
    }

    @Override
    public EdgeManager getEdgeManager() {
        return edgeManager;
    }

    @Override
    public ExecutionVertex getExecutionVertexOrThrow(ExecutionVertexID id) {
        return checkNotNull(executionVerticesById.get(id));
    }

    @Override
    public IntermediateResultPartition getResultPartitionOrThrow(
            final IntermediateResultPartitionID id) {
        return checkNotNull(resultPartitionsById.get(id));
    }

    @Override
    public long getStatusTimestamp(JobStatus status) {
        return this.stateTimestamps[status.ordinal()];
    }

    @Override
    public final BlobWriter getBlobWriter() {
        return blobWriter;
    }

    @Override
    public Executor getFutureExecutor() {
        return futureExecutor;
    }

    @Override
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

    @Override
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

    @Override
    public void notifyNewlyInitializedJobVertices(List<ExecutionJobVertex> vertices) {
        executionTopology.notifyExecutionGraphUpdated(this, vertices);
    }

    @Override
    public void attachJobGraph(List<JobVertex> verticesToAttach) throws JobException {

        assertRunningInJobMasterMainThread();

        LOG.debug(
                "Attaching {} topologically sorted vertices to existing job graph with {} "
                        + "vertices and {} intermediate results.",
                verticesToAttach.size(),
                tasks.size(),
                intermediateResults.size());

        attachJobVertices(verticesToAttach);
        if (!isDynamic) {
            initializeJobVertices(verticesToAttach);
        }

        // the topology assigning should happen before notifying new vertices to failoverStrategy
        executionTopology = DefaultExecutionTopology.fromExecutionGraph(this);

        partitionGroupReleaseStrategy =
                partitionGroupReleaseStrategyFactory.createInstance(getSchedulingTopology());
    }

    /** Attach job vertices without initializing them. */
    private void attachJobVertices(List<JobVertex> topologicallySorted) throws JobException {
        for (JobVertex jobVertex : topologicallySorted) {

            if (jobVertex.isInputVertex() && !jobVertex.isStoppable()) {
                this.isStoppable = false;
            }

            VertexParallelismInformation parallelismInfo =
                    parallelismStore.getParallelismInfo(jobVertex.getID());

            // create the execution job vertex and attach it to the graph
            ExecutionJobVertex ejv =
                    executionJobVertexFactory.createExecutionJobVertex(
                            this, jobVertex, parallelismInfo);

            ExecutionJobVertex previousTask = this.tasks.putIfAbsent(jobVertex.getID(), ejv);
            if (previousTask != null) {
                throw new JobException(
                        String.format(
                                "Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
                                jobVertex.getID(), ejv, previousTask));
            }

            this.verticesInCreationOrder.add(ejv);
            this.numJobVerticesTotal++;
        }
    }

    private void initializeJobVertices(List<JobVertex> topologicallySorted) throws JobException {
        final long createTimestamp = System.currentTimeMillis();

        for (JobVertex jobVertex : topologicallySorted) {
            final ExecutionJobVertex ejv = tasks.get(jobVertex.getID());
            initializeJobVertex(ejv, createTimestamp);
        }
    }

    @Override
    public void initializeJobVertex(
            ExecutionJobVertex ejv,
            long createTimestamp,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos)
            throws JobException {

        checkNotNull(ejv);
        checkNotNull(jobVertexInputInfos);

        jobVertexInputInfos.forEach(
                (resultId, info) ->
                        this.vertexInputInfoStore.put(ejv.getJobVertexId(), resultId, info));

        ejv.initialize(
                executionHistorySizeLimit,
                rpcTimeout,
                createTimestamp,
                this.initialAttemptCounts.getAttemptCounts(ejv.getJobVertexId()),
                coordinatorStore);

        ejv.connectToPredecessors(this.intermediateResults);

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

        registerExecutionVerticesAndResultPartitionsFor(ejv);

        // enrich network memory.
        SlotSharingGroup slotSharingGroup = ejv.getSlotSharingGroup();
        if (areJobVerticesAllInitialized(slotSharingGroup)) {
            SsgNetworkMemoryCalculationUtils.enrichNetworkMemory(
                    slotSharingGroup, this::getJobVertex, shuffleMaster);
        }
    }

    private boolean areJobVerticesAllInitialized(final SlotSharingGroup group) {
        for (JobVertexID jobVertexId : group.getJobVertexIds()) {
            final ExecutionJobVertex jobVertex = getJobVertex(jobVertexId);
            checkNotNull(jobVertex, "Unknown job vertex %s", jobVertexId);
            if (!jobVertex.isInitialized()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void transitionToRunning() {
        if (!transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
            throw new IllegalStateException(
                    "Job may only be scheduled from state " + JobStatus.CREATED);
        }
    }

    @Override
    public void cancel() {

        assertRunningInJobMasterMainThread();

        while (true) {
            JobStatus current = state;

            if (current == JobStatus.RUNNING
                    || current == JobStatus.CREATED
                    || current == JobStatus.RESTARTING) {
                if (transitionState(current, JobStatus.CANCELLING)) {

                    incrementRestarts();

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
                                    allVerticesInTerminalState();
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

    @VisibleForTesting
    protected ConjunctFuture<Void> cancelVerticesAsync() {
        final ArrayList<CompletableFuture<?>> futures =
                new ArrayList<>(verticesInCreationOrder.size());

        // cancel all tasks (that still need cancelling)
        for (ExecutionJobVertex ejv : verticesInCreationOrder) {
            futures.add(ejv.cancelWithFuture());
        }

        // we build a future that is complete once all vertices have reached a terminal state
        return FutureUtils.waitForAll(futures);
    }

    @Override
    public void suspend(Throwable suspensionCause) {

        assertRunningInJobMasterMainThread();

        if (state.isTerminalState()) {
            // stay in a terminal state
            return;
        } else if (transitionState(state, JobStatus.SUSPENDED, suspensionCause)) {
            initFailureCause(suspensionCause, System.currentTimeMillis());

            incrementRestarts();

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
        if (failedExecution != null
                && (failedExecution.getState() == ExecutionState.RUNNING
                        || failedExecution.getState() == ExecutionState.INITIALIZING)) {
            failGlobal(cause);
        } else {
            LOG.debug(
                    "The failing attempt {} belongs to an already not"
                            + " running task thus won't fail the job",
                    failingAttempt);
        }
    }

    @Override
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

    @Override
    public CompletableFuture<JobStatus> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
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

    // ------------------------------------------------------------------------
    //  State Transitions
    // ------------------------------------------------------------------------

    @Override
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
            notifyJobStatusChange(newState);
            notifyJobStatusHooks(newState, error);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void incrementRestarts() {
        numberOfRestartsCounter.inc();
    }

    @Override
    public void initFailureCause(Throwable t, long timestamp) {
        this.failureCause = t;
        this.failureInfo = new ErrorInfo(t, timestamp);
    }

    // ------------------------------------------------------------------------
    //  Job Status Progress
    // ------------------------------------------------------------------------

    /**
     * Called whenever a job vertex reaches state FINISHED (completed successfully). Once all job
     * vertices are in the FINISHED state, the program is successfully done.
     */
    @Override
    public void jobVertexFinished() {
        assertRunningInJobMasterMainThread();
        final int numFinished = ++numFinishedJobVertices;
        if (numFinished == numJobVerticesTotal) {
            FutureUtils.assertNoException(
                    waitForAllExecutionsTermination().thenAccept(ignored -> jobFinished()));
        }
    }

    private CompletableFuture<?> waitForAllExecutionsTermination() {
        final List<CompletableFuture<?>> terminationFutures =
                verticesInCreationOrder.stream()
                        .flatMap(ejv -> Arrays.stream(ejv.getTaskVertices()))
                        .map(ExecutionVertex::getTerminationFuture)
                        .collect(Collectors.toList());

        return FutureUtils.waitForAll(terminationFutures);
    }

    private void jobFinished() {
        assertRunningInJobMasterMainThread();

        // check whether we are still in "RUNNING" and trigger the final cleanup
        if (state == JobStatus.RUNNING) {
            // we do the final cleanup in the I/O executor, because it may involve
            // some heavier work

            try {
                for (ExecutionJobVertex ejv : verticesInCreationOrder) {
                    final Map<Integer, Integer> subtaskToFinishedAttempt =
                            Arrays.stream(ejv.getTaskVertices())
                                    .map(ExecutionVertex::getCurrentExecutionAttempt)
                                    .collect(
                                            Collectors.toMap(
                                                    Execution::getParallelSubtaskIndex,
                                                    Execution::getAttemptNumber));
                    ejv.getJobVertex()
                            .finalizeOnMaster(
                                    new FinalizeOnMasterContext() {
                                        @Override
                                        public ClassLoader getClassLoader() {
                                            return getUserClassLoader();
                                        }

                                        @Override
                                        public int getExecutionParallelism() {
                                            return ejv.getParallelism();
                                        }

                                        @Override
                                        public int getFinishedAttempt(int subtaskIndex) {
                                            final Integer attemptNumber =
                                                    subtaskToFinishedAttempt.get(subtaskIndex);
                                            if (attemptNumber == null) {
                                                throw new IllegalArgumentException(
                                                        "Invalid subtaskIndex "
                                                                + subtaskIndex
                                                                + " provided");
                                            }
                                            return attemptNumber;
                                        }
                                    });
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

    @Override
    public void jobVertexUnFinished() {
        assertRunningInJobMasterMainThread();
        numFinishedJobVertices--;
    }

    /**
     * This method is a callback during cancellation/failover and called when all tasks have reached
     * a terminal state (cancelled/failed/finished).
     */
    private void allVerticesInTerminalState() {

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

    @Override
    public void failJob(Throwable cause, long timestamp) {
        if (state == JobStatus.FAILING || state.isTerminalState()) {
            return;
        }

        transitionState(JobStatus.FAILING, cause);
        initFailureCause(cause, timestamp);

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
        LOG.debug("ExecutionGraph {} reached terminal state {}.", getJobID(), status);

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
            if (flushEventTimer != null) {
                flushEventTimer.shutdownNow();
                flushEventTimer = null;
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

    @Override
    public boolean updateState(TaskExecutionStateTransition state) {
        assertRunningInJobMasterMainThread();
        final Execution attempt = currentExecutions.get(state.getID());

        if (attempt != null) {
            try {
                final boolean stateUpdated = updateStateInternal(state, attempt);
                maybeReleasePartitionGroupsFor(attempt);
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
            case INITIALIZING:
                return attempt.switchToRecovering();

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

    private void maybeReleasePartitionGroupsFor(final Execution attempt) {
        final ExecutionVertexID finishedExecutionVertex = attempt.getVertex().getID();

        if (attempt.getState() == ExecutionState.FINISHED) {
            final List<ConsumedPartitionGroup> releasablePartitionGroups =
                    partitionGroupReleaseStrategy.vertexFinished(finishedExecutionVertex);
            releasePartitionGroups(releasablePartitionGroups);
        } else {
            partitionGroupReleaseStrategy.vertexUnfinished(finishedExecutionVertex);
        }
    }

    private void releasePartitionGroups(
            final List<ConsumedPartitionGroup> releasablePartitionGroups) {

        if (releasablePartitionGroups.size() > 0) {
            final List<ResultPartitionID> releasablePartitionIds = new ArrayList<>();

            // Remove the cache of ShuffleDescriptors when ConsumedPartitionGroups are released
            for (ConsumedPartitionGroup releasablePartitionGroup : releasablePartitionGroups) {
                IntermediateResult totalResult =
                        checkNotNull(
                                intermediateResults.get(
                                        releasablePartitionGroup.getIntermediateDataSetID()));
                for (IntermediateResultPartitionID partitionId : releasablePartitionGroup) {
                    IntermediateResultPartition partition =
                            totalResult.getPartitionById(partitionId);
                    partition.markPartitionGroupReleasable(releasablePartitionGroup);
                    if (partition.canBeReleased()) {
                        releasablePartitionIds.add(createResultPartitionId(partitionId));
                    }
                }
                totalResult.clearCachedInformationForPartitionGroup(releasablePartitionGroup);
            }

            partitionTracker.stopTrackingAndReleasePartitions(releasablePartitionIds);
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

    @Override
    public Map<ExecutionAttemptID, Execution> getRegisteredExecutions() {
        return Collections.unmodifiableMap(currentExecutions);
    }

    @Override
    public void registerExecution(Execution exec) {
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

    @Override
    public void deregisterExecution(Execution exec) {
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

    private void registerExecutionVerticesAndResultPartitionsFor(
            ExecutionJobVertex executionJobVertex) {
        for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
            executionVerticesById.put(executionVertex.getID(), executionVertex);
            resultPartitionsById.putAll(executionVertex.getProducedPartitions());
        }
    }

    @Override
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

    @Override
    public void registerJobStatusListener(JobStatusListener listener) {
        if (listener != null) {
            jobStatusListeners.add(listener);
        }
    }

    private void notifyJobStatusChange(JobStatus newState) {
        if (jobStatusListeners.size() > 0) {
            final long timestamp = System.currentTimeMillis();

            for (JobStatusListener listener : jobStatusListeners) {
                try {
                    listener.jobStatusChanges(getJobID(), newState, timestamp);
                } catch (Throwable t) {
                    LOG.warn("Error while notifying JobStatusListener", t);
                }
            }
        }
    }

    private void notifyJobStatusHooks(JobStatus newState, @Nullable Throwable cause) {
        JobID jobID = jobInformation.getJobId();
        for (JobStatusHook hook : jobStatusHooks) {
            try {
                switch (newState) {
                    case CREATED:
                        hook.onCreated(jobID);
                        break;
                    case CANCELED:
                        hook.onCanceled(jobID);
                        break;
                    case FAILED:
                        hook.onFailed(jobID, cause);
                        break;
                    case FINISHED:
                        hook.onFinished(jobID);
                        break;
                }
            } catch (Throwable e) {
                throw new RuntimeException(
                        "Error while notifying JobStatusHook[" + hook.getClass() + "]", e);
            }
        }
    }

    @Override
    public void notifyExecutionChange(
            final Execution execution,
            ExecutionState previousState,
            final ExecutionState newExecutionState) {
        executionStateUpdateListener.onStateUpdate(
                execution.getAttemptId(), previousState, newExecutionState);
    }

    private void assertRunningInJobMasterMainThread() {
        if (!(jobMasterMainThreadExecutor
                instanceof ComponentMainThreadExecutor.DummyComponentMainThreadExecutor)) {
            jobMasterMainThreadExecutor.assertRunningInMainThread();
        }
    }

    @Override
    public void notifySchedulerNgAboutInternalTaskFailure(
            final ExecutionAttemptID attemptId,
            final Throwable t,
            final boolean cancelTask,
            final boolean releasePartitions) {
        checkState(internalTaskFailuresListener != null);
        internalTaskFailuresListener.notifyTaskFailure(attemptId, t, cancelTask, releasePartitions);
    }

    @Override
    public void deleteBlobs(List<PermanentBlobKey> blobKeys) {
        CompletableFuture.runAsync(
                () -> {
                    for (PermanentBlobKey blobKey : blobKeys) {
                        blobWriter.deletePermanent(getJobID(), blobKey);
                    }
                },
                ioExecutor);
    }

    @Override
    public ShuffleMaster<?> getShuffleMaster() {
        return shuffleMaster;
    }

    @Override
    public JobMasterPartitionTracker getPartitionTracker() {
        return partitionTracker;
    }

    @Override
    public ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
        return resultPartitionAvailabilityChecker;
    }

    @Override
    public PartitionGroupReleaseStrategy getPartitionGroupReleaseStrategy() {
        return partitionGroupReleaseStrategy;
    }

    @Override
    public ExecutionDeploymentListener getExecutionDeploymentListener() {
        return executionDeploymentListener;
    }

    @Override
    public boolean isDynamic() {
        return isDynamic;
    }

    @Override
    public Optional<String> findVertexWithAttempt(ExecutionAttemptID attemptId) {
        return Optional.ofNullable(currentExecutions.get(attemptId))
                .map(Execution::getVertexWithAttempt);
    }

    @Override
    public Optional<AccessExecution> findExecution(ExecutionAttemptID attemptId) {
        return Optional.ofNullable(currentExecutions.get(attemptId));
    }

    @Override
    public ExecutionGraphID getExecutionGraphID() {
        return executionGraphId;
    }

    @Override
    public List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID) {
        return partitionTracker.getClusterPartitionShuffleDescriptors(intermediateDataSetID);
    }

    @Override
    public MarkPartitionFinishedStrategy getMarkPartitionFinishedStrategy() {
        return markPartitionFinishedStrategy;
    }

    @Override
    public JobVertexInputInfo getJobVertexInputInfo(
            JobVertexID jobVertexId, IntermediateDataSetID resultId) {
        return vertexInputInfoStore.get(jobVertexId, resultId);
    }

    @Override
    public TaskDeploymentDescriptorFactory getTaskDeploymentDescriptorFactory() {
        return taskDeploymentDescriptorFactory;
    }
}

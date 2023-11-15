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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.FinishedTaskStateProvider.PartialFinishingNotSupportedByStateException;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.FULL_CHECKPOINT;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint coordinator coordinates the distributed snapshots of operators and state. It
 * triggers the checkpoint by sending the messages to the relevant tasks and collects the checkpoint
 * acknowledgements. It also collects and maintains the overview of the state handles reported by
 * the tasks that acknowledge the checkpoint.
 */
public class CheckpointCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    /** The number of recent checkpoints whose IDs are remembered. */
    private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

    // ------------------------------------------------------------------------

    /** Coordinator-wide lock to safeguard the checkpoint updates. */
    private final Object lock = new Object();

    /** The job whose checkpoint this coordinator coordinates. */
    private final JobID job;

    /** Default checkpoint properties. */
    private final CheckpointProperties checkpointProperties;

    /** The executor used for asynchronous calls, like potentially blocking I/O. */
    private final Executor executor;

    private final CheckpointsCleaner checkpointsCleaner;

    /** The operator coordinators that need to be checkpointed. */
    private final Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint;

    /** Map from checkpoint ID to the pending checkpoint. */
    @GuardedBy("lock")
    private final Map<Long, PendingCheckpoint> pendingCheckpoints;

    /**
     * Completed checkpoints. Implementations can be blocking. Make sure calls to methods accessing
     * this don't block the job manager actor and run asynchronously.
     */
    private final CompletedCheckpointStore completedCheckpointStore;

    /**
     * The root checkpoint state backend, which is responsible for initializing the checkpoint,
     * storing the metadata, and cleaning up the checkpoint.
     */
    private final CheckpointStorageCoordinatorView checkpointStorageView;

    /** A list of recent expired checkpoint IDs, to identify late messages (vs invalid ones). */
    private final ArrayDeque<Long> recentExpiredCheckpoints;

    /**
     * Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these need to
     * be ascending across job managers.
     */
    private final CheckpointIDCounter checkpointIdCounter;

    /**
     * The checkpoint interval when there is no source reporting isProcessingBacklog=true. Actual
     * trigger time may be affected by the max concurrent checkpoints, minimum-pause values and
     * checkpoint interval during backlog.
     */
    private final long baseInterval;

    /**
     * The checkpoint interval when any source reports isProcessingBacklog=true. Actual trigger time
     * may be affected by the max concurrent checkpoints and minimum-pause values.
     */
    private final long baseIntervalDuringBacklog;

    /** The max time (in ms) that a checkpoint may take. */
    private final long checkpointTimeout;

    /**
     * The min time(in ms) to delay after a checkpoint could be triggered. Allows to enforce minimum
     * processing time between checkpoint attempts
     */
    private final long minPauseBetweenCheckpoints;

    /**
     * The timer that handles the checkpoint timeouts and triggers periodic checkpoints. It must be
     * single-threaded. Eventually it will be replaced by main thread executor.
     */
    private final ScheduledExecutor timer;

    /** The master checkpoint hooks executed by this checkpoint coordinator. */
    private final HashMap<String, MasterTriggerRestoreHook<?>> masterHooks;

    private final boolean unalignedCheckpointsEnabled;

    private final long alignedCheckpointTimeout;

    /** Actor that receives status updates from the execution graph this coordinator works for. */
    private JobStatusListener jobStatusListener;

    /**
     * The current periodic trigger. Used to deduplicate concurrently scheduled checkpoints if any.
     */
    @GuardedBy("lock")
    private ScheduledTrigger currentPeriodicTrigger;

    /** A handle to the current periodic trigger, to cancel it when necessary. */
    @GuardedBy("lock")
    private ScheduledFuture<?> currentPeriodicTriggerFuture;

    /**
     * The timestamp (via {@link Clock#relativeTimeMillis()}) when the next checkpoint will be
     * triggered.
     *
     * <p>If it's value is {@link Long#MAX_VALUE}, it means there is not a next checkpoint
     * scheduled.
     */
    @GuardedBy("lock")
    private long nextCheckpointTriggeringRelativeTime;

    /**
     * The timestamp (via {@link Clock#relativeTimeMillis()}) when the last checkpoint completed.
     */
    private long lastCheckpointCompletionRelativeTime;

    /**
     * Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
     * Non-volatile, because only accessed in synchronized scope
     */
    private boolean periodicScheduling;

    /** Flag marking the coordinator as shut down (not accepting any messages any more). */
    private volatile boolean shutdown;

    /** Optional tracker for checkpoint statistics. */
    private final CheckpointStatsTracker statsTracker;

    private final BiFunction<
                    Set<ExecutionJobVertex>,
                    Map<OperatorID, OperatorState>,
                    VertexFinishedStateChecker>
            vertexFinishedStateCheckerFactory;

    /** Id of checkpoint for which in-flight data should be ignored on recovery. */
    private final long checkpointIdOfIgnoredInFlightData;

    private final CheckpointFailureManager failureManager;

    private final Clock clock;

    private final boolean isExactlyOnceMode;

    /** Flag represents there is an in-flight trigger request. */
    private boolean isTriggering = false;

    private final CheckpointRequestDecider requestDecider;

    private final CheckpointPlanCalculator checkpointPlanCalculator;

    /** IDs of the source operators that are currently processing backlog. */
    @GuardedBy("lock")
    private final Set<OperatorID> backlogOperators = new HashSet<>();

    private boolean baseLocationsForCheckpointInitialized = false;

    private boolean forceFullSnapshot;

    // --------------------------------------------------------------------------------------------

    public CheckpointCoordinator(
            JobID job,
            CheckpointCoordinatorConfiguration chkConfig,
            Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointStorage checkpointStorage,
            Executor executor,
            CheckpointsCleaner checkpointsCleaner,
            ScheduledExecutor timer,
            CheckpointFailureManager failureManager,
            CheckpointPlanCalculator checkpointPlanCalculator,
            CheckpointStatsTracker statsTracker) {

        this(
                job,
                chkConfig,
                coordinatorsToCheckpoint,
                checkpointIDCounter,
                completedCheckpointStore,
                checkpointStorage,
                executor,
                checkpointsCleaner,
                timer,
                failureManager,
                checkpointPlanCalculator,
                SystemClock.getInstance(),
                statsTracker,
                VertexFinishedStateChecker::new);
    }

    @VisibleForTesting
    public CheckpointCoordinator(
            JobID job,
            CheckpointCoordinatorConfiguration chkConfig,
            Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointStorage checkpointStorage,
            Executor executor,
            CheckpointsCleaner checkpointsCleaner,
            ScheduledExecutor timer,
            CheckpointFailureManager failureManager,
            CheckpointPlanCalculator checkpointPlanCalculator,
            Clock clock,
            CheckpointStatsTracker statsTracker,
            BiFunction<
                            Set<ExecutionJobVertex>,
                            Map<OperatorID, OperatorState>,
                            VertexFinishedStateChecker>
                    vertexFinishedStateCheckerFactory) {

        // sanity checks
        checkNotNull(checkpointStorage);

        // max "in between duration" can be one year - this is to prevent numeric overflows
        long minPauseBetweenCheckpoints = chkConfig.getMinPauseBetweenCheckpoints();
        if (minPauseBetweenCheckpoints > 365L * 24 * 60 * 60 * 1_000) {
            minPauseBetweenCheckpoints = 365L * 24 * 60 * 60 * 1_000;
        }

        // it does not make sense to schedule checkpoints more often then the desired
        // time between checkpoints
        long baseInterval = chkConfig.getCheckpointInterval();
        if (baseInterval < minPauseBetweenCheckpoints) {
            baseInterval = minPauseBetweenCheckpoints;
        }

        this.job = checkNotNull(job);
        this.baseInterval = baseInterval;
        this.baseIntervalDuringBacklog = chkConfig.getCheckpointIntervalDuringBacklog();
        this.nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
        this.checkpointTimeout = chkConfig.getCheckpointTimeout();
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
        this.coordinatorsToCheckpoint =
                Collections.unmodifiableCollection(coordinatorsToCheckpoint);
        this.pendingCheckpoints = new LinkedHashMap<>();
        this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
        this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
        this.executor = checkNotNull(executor);
        this.checkpointsCleaner = checkNotNull(checkpointsCleaner);
        this.failureManager = checkNotNull(failureManager);
        this.checkpointPlanCalculator = checkNotNull(checkpointPlanCalculator);
        this.clock = checkNotNull(clock);
        this.isExactlyOnceMode = chkConfig.isExactlyOnce();
        this.unalignedCheckpointsEnabled = chkConfig.isUnalignedCheckpointsEnabled();
        this.alignedCheckpointTimeout = chkConfig.getAlignedCheckpointTimeout();
        this.checkpointIdOfIgnoredInFlightData = chkConfig.getCheckpointIdOfIgnoredInFlightData();

        this.recentExpiredCheckpoints = new ArrayDeque<>(NUM_GHOST_CHECKPOINT_IDS);
        this.masterHooks = new HashMap<>();

        this.timer = timer;

        this.checkpointProperties =
                CheckpointProperties.forCheckpoint(chkConfig.getCheckpointRetentionPolicy());

        try {
            this.checkpointStorageView = checkpointStorage.createCheckpointStorage(job);

            if (isPeriodicCheckpointingConfigured()) {
                checkpointStorageView.initializeBaseLocationsForCheckpoint();
                baseLocationsForCheckpointInitialized = true;
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Failed to create checkpoint storage at checkpoint coordinator side.", e);
        }

        try {
            // Make sure the checkpoint ID enumerator is running. Possibly
            // issues a blocking call to ZooKeeper.
            checkpointIDCounter.start();
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Failed to start checkpoint ID counter: " + t.getMessage(), t);
        }
        this.requestDecider =
                new CheckpointRequestDecider(
                        chkConfig.getMaxConcurrentCheckpoints(),
                        this::rescheduleTrigger,
                        this.clock,
                        this.minPauseBetweenCheckpoints,
                        this.pendingCheckpoints::size,
                        this.checkpointsCleaner::getNumberOfCheckpointsToClean);
        this.statsTracker = checkNotNull(statsTracker, "Statistic tracker can not be null");
        this.vertexFinishedStateCheckerFactory = checkNotNull(vertexFinishedStateCheckerFactory);
    }

    // --------------------------------------------------------------------------------------------
    //  Configuration
    // --------------------------------------------------------------------------------------------

    /**
     * Adds the given master hook to the checkpoint coordinator. This method does nothing, if the
     * checkpoint coordinator already contained a hook with the same ID (as defined via {@link
     * MasterTriggerRestoreHook#getIdentifier()}).
     *
     * @param hook The hook to add.
     * @return True, if the hook was added, false if the checkpoint coordinator already contained a
     *     hook with the same ID.
     */
    public boolean addMasterHook(MasterTriggerRestoreHook<?> hook) {
        checkNotNull(hook);

        final String id = hook.getIdentifier();
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(id), "The hook has a null or empty id");

        synchronized (lock) {
            if (!masterHooks.containsKey(id)) {
                masterHooks.put(id, hook);
                return true;
            } else {
                return false;
            }
        }
    }

    /** Gets the number of currently register master hooks. */
    public int getNumberOfRegisteredMasterHooks() {
        synchronized (lock) {
            return masterHooks.size();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Clean shutdown
    // --------------------------------------------------------------------------------------------

    /**
     * Shuts down the checkpoint coordinator.
     *
     * <p>After this method has been called, the coordinator does not accept and further messages
     * and cannot trigger any further checkpoints.
     */
    public void shutdown() throws Exception {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;
                LOG.info("Stopping checkpoint coordinator for job {}.", job);

                periodicScheduling = false;

                // shut down the hooks
                MasterHooks.close(masterHooks.values(), LOG);
                masterHooks.clear();

                final CheckpointException reason =
                        new CheckpointException(
                                CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
                // clear queued requests and in-flight checkpoints
                abortPendingAndQueuedCheckpoints(reason);
            }
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Reports whether a source operator is currently processing backlog.
     *
     * <p>If any source operator is processing backlog, the checkpoint interval would be decided by
     * {@code execution.checkpointing.interval-during-backlog} instead of {@code
     * execution.checkpointing.interval}.
     *
     * <p>If a source has not invoked this method, the source is considered to have
     * isProcessingBacklog=false. If a source operator has invoked this method multiple times, the
     * last reported value is used.
     *
     * @param operatorID the operator ID of the source operator.
     * @param isProcessingBacklog whether the source operator is processing backlog.
     */
    public void setIsProcessingBacklog(OperatorID operatorID, boolean isProcessingBacklog) {
        synchronized (lock) {
            if (isProcessingBacklog) {
                backlogOperators.add(operatorID);
            } else {
                backlogOperators.remove(operatorID);
            }

            long currentCheckpointInterval = getCurrentCheckpointInterval();
            if (currentCheckpointInterval
                    != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL) {
                long currentRelativeTime = clock.relativeTimeMillis();
                if (currentRelativeTime + currentCheckpointInterval
                        < nextCheckpointTriggeringRelativeTime) {
                    rescheduleTrigger(currentRelativeTime, currentCheckpointInterval);
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Triggering Checkpoints and Savepoints
    // --------------------------------------------------------------------------------------------

    /**
     * Triggers a savepoint with the given savepoint directory as a target.
     *
     * @param targetLocation Target location for the savepoint, optional. If null, the state
     *     backend's configured default will be used.
     * @return A future to the completed checkpoint
     * @throws IllegalStateException If no savepoint directory has been specified and no default
     *     savepoint directory has been configured
     */
    public CompletableFuture<CompletedCheckpoint> triggerSavepoint(
            @Nullable final String targetLocation, final SavepointFormatType formatType) {
        final CheckpointProperties properties =
                CheckpointProperties.forSavepoint(!unalignedCheckpointsEnabled, formatType);
        return triggerSavepointInternal(properties, targetLocation);
    }

    /**
     * Triggers a synchronous savepoint with the given savepoint directory as a target.
     *
     * @param terminate flag indicating if the job should terminate or just suspend
     * @param targetLocation Target location for the savepoint, optional. If null, the state
     *     backend's configured default will be used.
     * @return A future to the completed checkpoint
     * @throws IllegalStateException If no savepoint directory has been specified and no default
     *     savepoint directory has been configured
     */
    public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
            final boolean terminate,
            @Nullable final String targetLocation,
            SavepointFormatType formatType) {

        final CheckpointProperties properties =
                CheckpointProperties.forSyncSavepoint(
                        !unalignedCheckpointsEnabled, terminate, formatType);

        return triggerSavepointInternal(properties, targetLocation);
    }

    private CompletableFuture<CompletedCheckpoint> triggerSavepointInternal(
            final CheckpointProperties checkpointProperties,
            @Nullable final String targetLocation) {

        checkNotNull(checkpointProperties);

        return triggerCheckpointFromCheckpointThread(checkpointProperties, targetLocation, false);
    }

    private CompletableFuture<CompletedCheckpoint> triggerCheckpointFromCheckpointThread(
            CheckpointProperties checkpointProperties, String targetLocation, boolean isPeriodic) {
        // TODO, call triggerCheckpoint directly after removing timer thread
        // for now, execute the trigger in timer thread to avoid competition
        final CompletableFuture<CompletedCheckpoint> resultFuture = new CompletableFuture<>();
        timer.execute(
                () ->
                        triggerCheckpoint(checkpointProperties, targetLocation, isPeriodic)
                                .whenComplete(
                                        (completedCheckpoint, throwable) -> {
                                            if (throwable == null) {
                                                resultFuture.complete(completedCheckpoint);
                                            } else {
                                                resultFuture.completeExceptionally(throwable);
                                            }
                                        }));
        return resultFuture;
    }

    /**
     * Triggers a new standard checkpoint and uses the given timestamp as the checkpoint timestamp.
     * The return value is a future. It completes when the checkpoint triggered finishes or an error
     * occurred.
     *
     * @param isPeriodic Flag indicating whether this triggered checkpoint is periodic.
     * @return a future to the completed checkpoint.
     */
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(boolean isPeriodic) {
        return triggerCheckpointFromCheckpointThread(checkpointProperties, null, isPeriodic);
    }

    /**
     * Triggers one new checkpoint with the given checkpointType. The returned future completes when
     * the triggered checkpoint finishes or an error occurred.
     *
     * @param checkpointType specifies the backup type of the checkpoint to trigger.
     * @return a future to the completed checkpoint.
     */
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {

        if (checkpointType == null) {
            throw new IllegalArgumentException("checkpointType cannot be null");
        }

        final SnapshotType snapshotType;
        switch (checkpointType) {
            case CONFIGURED:
                snapshotType = checkpointProperties.getCheckpointType();
                break;
            case FULL:
                snapshotType = FULL_CHECKPOINT;
                break;
            case INCREMENTAL:
                snapshotType = CHECKPOINT;
                break;
            default:
                throw new IllegalArgumentException("unknown checkpointType: " + checkpointType);
        }

        final CheckpointProperties properties =
                new CheckpointProperties(
                        checkpointProperties.forceCheckpoint(),
                        snapshotType,
                        checkpointProperties.discardOnSubsumed(),
                        checkpointProperties.discardOnJobFinished(),
                        checkpointProperties.discardOnJobCancelled(),
                        checkpointProperties.discardOnJobFailed(),
                        checkpointProperties.discardOnJobSuspended(),
                        checkpointProperties.isUnclaimed());
        return triggerCheckpointFromCheckpointThread(properties, null, false);
    }

    @VisibleForTesting
    CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            CheckpointProperties props,
            @Nullable String externalSavepointLocation,
            boolean isPeriodic) {

        CheckpointTriggerRequest request =
                new CheckpointTriggerRequest(props, externalSavepointLocation, isPeriodic);
        chooseRequestToExecute(request).ifPresent(this::startTriggeringCheckpoint);
        return request.onCompletionPromise;
    }

    private void startTriggeringCheckpoint(CheckpointTriggerRequest request) {
        try {
            synchronized (lock) {
                preCheckGlobalState(request.isPeriodic);
            }

            // we will actually trigger this checkpoint!
            Preconditions.checkState(!isTriggering);
            isTriggering = true;

            final long timestamp = System.currentTimeMillis();

            CompletableFuture<CheckpointPlan> checkpointPlanFuture =
                    checkpointPlanCalculator.calculateCheckpointPlan();

            boolean initializeBaseLocations = !baseLocationsForCheckpointInitialized;
            baseLocationsForCheckpointInitialized = true;

            CompletableFuture<Void> masterTriggerCompletionPromise = new CompletableFuture<>();

            final CompletableFuture<PendingCheckpoint> pendingCheckpointCompletableFuture =
                    checkpointPlanFuture
                            .thenApplyAsync(
                                    plan -> {
                                        try {
                                            // this must happen outside the coordinator-wide lock,
                                            // because it communicates with external services
                                            // (in HA mode) and may block for a while.
                                            long checkpointID =
                                                    checkpointIdCounter.getAndIncrement();
                                            return new Tuple2<>(plan, checkpointID);
                                        } catch (Throwable e) {
                                            throw new CompletionException(e);
                                        }
                                    },
                                    executor)
                            .thenApplyAsync(
                                    (checkpointInfo) ->
                                            createPendingCheckpoint(
                                                    timestamp,
                                                    request.props,
                                                    checkpointInfo.f0,
                                                    request.isPeriodic,
                                                    checkpointInfo.f1,
                                                    request.getOnCompletionFuture(),
                                                    masterTriggerCompletionPromise),
                                    timer);

            final CompletableFuture<?> coordinatorCheckpointsComplete =
                    pendingCheckpointCompletableFuture
                            .thenApplyAsync(
                                    pendingCheckpoint -> {
                                        try {
                                            CheckpointStorageLocation checkpointStorageLocation =
                                                    initializeCheckpointLocation(
                                                            pendingCheckpoint.getCheckpointID(),
                                                            request.props,
                                                            request.externalSavepointLocation,
                                                            initializeBaseLocations);
                                            return Tuple2.of(
                                                    pendingCheckpoint, checkpointStorageLocation);
                                        } catch (Throwable e) {
                                            throw new CompletionException(e);
                                        }
                                    },
                                    executor)
                            .thenComposeAsync(
                                    (checkpointInfo) -> {
                                        PendingCheckpoint pendingCheckpoint = checkpointInfo.f0;
                                        if (pendingCheckpoint.isDisposed()) {
                                            // The disposed checkpoint will be handled later,
                                            // skip snapshotting the coordinator states.
                                            return null;
                                        }
                                        synchronized (lock) {
                                            pendingCheckpoint.setCheckpointTargetLocation(
                                                    checkpointInfo.f1);
                                        }
                                        return OperatorCoordinatorCheckpoints
                                                .triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                                                        coordinatorsToCheckpoint,
                                                        pendingCheckpoint,
                                                        timer);
                                    },
                                    timer);

            // We have to take the snapshot of the master hooks after the coordinator checkpoints
            // has completed.
            // This is to ensure the tasks are checkpointed after the OperatorCoordinators in case
            // ExternallyInducedSource is used.
            final CompletableFuture<?> masterStatesComplete =
                    coordinatorCheckpointsComplete.thenComposeAsync(
                            ignored -> {
                                // If the code reaches here, the pending checkpoint is guaranteed to
                                // be not null.
                                // We use FutureUtils.getWithoutException() to make compiler happy
                                // with checked
                                // exceptions in the signature.
                                PendingCheckpoint checkpoint =
                                        FutureUtils.getWithoutException(
                                                pendingCheckpointCompletableFuture);
                                if (checkpoint == null || checkpoint.isDisposed()) {
                                    // The disposed checkpoint will be handled later,
                                    // skip snapshotting the master states.
                                    return null;
                                }
                                return snapshotMasterState(checkpoint);
                            },
                            timer);

            FutureUtils.forward(
                    CompletableFuture.allOf(masterStatesComplete, coordinatorCheckpointsComplete),
                    masterTriggerCompletionPromise);

            FutureUtils.assertNoException(
                    masterTriggerCompletionPromise
                            .handleAsync(
                                    (ignored, throwable) -> {
                                        final PendingCheckpoint checkpoint =
                                                FutureUtils.getWithoutException(
                                                        pendingCheckpointCompletableFuture);

                                        Preconditions.checkState(
                                                checkpoint != null || throwable != null,
                                                "Either the pending checkpoint needs to be created or an error must have occurred.");

                                        if (throwable != null) {
                                            // the initialization might not be finished yet
                                            if (checkpoint == null) {
                                                onTriggerFailure(request, throwable);
                                            } else {
                                                onTriggerFailure(checkpoint, throwable);
                                            }
                                        } else {
                                            triggerCheckpointRequest(
                                                    request, timestamp, checkpoint);
                                        }
                                        return null;
                                    },
                                    timer)
                            .exceptionally(
                                    error -> {
                                        if (!isShutdown()) {
                                            throw new CompletionException(error);
                                        } else if (findThrowable(
                                                        error, RejectedExecutionException.class)
                                                .isPresent()) {
                                            LOG.debug("Execution rejected during shutdown");
                                        } else {
                                            LOG.warn("Error encountered during shutdown", error);
                                        }
                                        return null;
                                    }));
        } catch (Throwable throwable) {
            onTriggerFailure(request, throwable);
        }
    }

    private void triggerCheckpointRequest(
            CheckpointTriggerRequest request, long timestamp, PendingCheckpoint checkpoint) {
        if (checkpoint.isDisposed()) {
            onTriggerFailure(
                    checkpoint,
                    new CheckpointException(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
                            checkpoint.getFailureCause()));
        } else {
            triggerTasks(request, timestamp, checkpoint)
                    .exceptionally(
                            failure -> {
                                LOG.info(
                                        "Triggering Checkpoint {} for job {} failed due to {}",
                                        checkpoint.getCheckpointID(),
                                        job,
                                        failure);

                                final CheckpointException cause;
                                if (failure instanceof CheckpointException) {
                                    cause = (CheckpointException) failure;
                                } else {
                                    cause =
                                            new CheckpointException(
                                                    CheckpointFailureReason
                                                            .TRIGGER_CHECKPOINT_FAILURE,
                                                    failure);
                                }
                                timer.execute(
                                        () -> {
                                            synchronized (lock) {
                                                abortPendingCheckpoint(checkpoint, cause);
                                            }
                                        });
                                return null;
                            });

            // It is possible that the tasks has finished
            // checkpointing at this point.
            // So we need to complete this pending checkpoint.
            if (maybeCompleteCheckpoint(checkpoint)) {
                onTriggerSuccess();
            }
        }
    }

    private CompletableFuture<Void> triggerTasks(
            CheckpointTriggerRequest request, long timestamp, PendingCheckpoint checkpoint) {
        // no exception, no discarding, everything is OK
        final long checkpointId = checkpoint.getCheckpointID();

        final SnapshotType type;
        if (this.forceFullSnapshot && !request.props.isSavepoint()) {
            type = FULL_CHECKPOINT;
        } else {
            type = request.props.getCheckpointType();
        }

        final CheckpointOptions checkpointOptions =
                CheckpointOptions.forConfig(
                        type,
                        checkpoint.getCheckpointStorageLocation().getLocationReference(),
                        isExactlyOnceMode,
                        unalignedCheckpointsEnabled,
                        alignedCheckpointTimeout);

        // send messages to the tasks to trigger their checkpoints
        List<CompletableFuture<Acknowledge>> acks = new ArrayList<>();
        for (Execution execution : checkpoint.getCheckpointPlan().getTasksToTrigger()) {
            if (request.props.isSynchronous()) {
                acks.add(
                        execution.triggerSynchronousSavepoint(
                                checkpointId, timestamp, checkpointOptions));
            } else {
                acks.add(execution.triggerCheckpoint(checkpointId, timestamp, checkpointOptions));
            }
        }
        return FutureUtils.waitForAll(acks);
    }

    /**
     * Initialize the checkpoint location asynchronously. It will be expected to be executed in io
     * thread due to it might be time-consuming.
     *
     * @param checkpointID checkpoint id
     * @param props checkpoint properties
     * @param externalSavepointLocation the external savepoint location, it might be null
     * @return the checkpoint location
     */
    private CheckpointStorageLocation initializeCheckpointLocation(
            long checkpointID,
            CheckpointProperties props,
            @Nullable String externalSavepointLocation,
            boolean initializeBaseLocations)
            throws Exception {
        final CheckpointStorageLocation checkpointStorageLocation;
        if (props.isSavepoint()) {
            checkpointStorageLocation =
                    checkpointStorageView.initializeLocationForSavepoint(
                            checkpointID, externalSavepointLocation);
        } else {
            if (initializeBaseLocations) {
                checkpointStorageView.initializeBaseLocationsForCheckpoint();
            }
            checkpointStorageLocation =
                    checkpointStorageView.initializeLocationForCheckpoint(checkpointID);
        }

        return checkpointStorageLocation;
    }

    private PendingCheckpoint createPendingCheckpoint(
            long timestamp,
            CheckpointProperties props,
            CheckpointPlan checkpointPlan,
            boolean isPeriodic,
            long checkpointID,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            CompletableFuture<Void> masterTriggerCompletionPromise) {

        synchronized (lock) {
            try {
                // since we haven't created the PendingCheckpoint yet, we need to check the
                // global state here.
                preCheckGlobalState(isPeriodic);
            } catch (Throwable t) {
                throw new CompletionException(t);
            }
        }

        PendingCheckpointStats pendingCheckpointStats =
                trackPendingCheckpointStats(checkpointID, checkpointPlan, props, timestamp);

        final PendingCheckpoint checkpoint =
                new PendingCheckpoint(
                        job,
                        checkpointID,
                        timestamp,
                        checkpointPlan,
                        OperatorInfo.getIds(coordinatorsToCheckpoint),
                        masterHooks.keySet(),
                        props,
                        onCompletionPromise,
                        pendingCheckpointStats,
                        masterTriggerCompletionPromise);

        synchronized (lock) {
            pendingCheckpoints.put(checkpointID, checkpoint);

            ScheduledFuture<?> cancellerHandle =
                    timer.schedule(
                            new CheckpointCanceller(checkpoint),
                            checkpointTimeout,
                            TimeUnit.MILLISECONDS);

            if (!checkpoint.setCancellerHandle(cancellerHandle)) {
                // checkpoint is already disposed!
                cancellerHandle.cancel(false);
            }
        }

        LOG.info(
                "Triggering checkpoint {} (type={}) @ {} for job {}.",
                checkpointID,
                checkpoint.getProps().getCheckpointType(),
                timestamp,
                job);
        return checkpoint;
    }

    /**
     * Snapshot master hook states asynchronously.
     *
     * @param checkpoint the pending checkpoint
     * @return the future represents master hook states are finished or not
     */
    private CompletableFuture<Void> snapshotMasterState(PendingCheckpoint checkpoint) {
        if (masterHooks.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final long checkpointID = checkpoint.getCheckpointID();
        final long timestamp = checkpoint.getCheckpointTimestamp();

        final CompletableFuture<Void> masterStateCompletableFuture = new CompletableFuture<>();
        for (MasterTriggerRestoreHook<?> masterHook : masterHooks.values()) {
            MasterHooks.triggerHook(masterHook, checkpointID, timestamp, executor)
                    .whenCompleteAsync(
                            (masterState, throwable) -> {
                                try {
                                    synchronized (lock) {
                                        if (masterStateCompletableFuture.isDone()) {
                                            return;
                                        }
                                        if (checkpoint.isDisposed()) {
                                            throw new IllegalStateException(
                                                    "Checkpoint "
                                                            + checkpointID
                                                            + " has been discarded");
                                        }
                                        if (throwable == null) {
                                            checkpoint.acknowledgeMasterState(
                                                    masterHook.getIdentifier(), masterState);
                                            if (checkpoint.areMasterStatesFullyAcknowledged()) {
                                                masterStateCompletableFuture.complete(null);
                                            }
                                        } else {
                                            masterStateCompletableFuture.completeExceptionally(
                                                    throwable);
                                        }
                                    }
                                } catch (Throwable t) {
                                    masterStateCompletableFuture.completeExceptionally(t);
                                }
                            },
                            timer);
        }
        return masterStateCompletableFuture;
    }

    /** Trigger request is successful. NOTE, it must be invoked if trigger request is successful. */
    private void onTriggerSuccess() {
        isTriggering = false;
        executeQueuedRequest();
    }

    /**
     * The trigger request is failed prematurely without a proper initialization. There is no
     * resource to release, but the completion promise needs to fail manually here.
     *
     * @param onCompletionPromise the completion promise of the checkpoint/savepoint
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(
            CheckpointTriggerRequest onCompletionPromise, Throwable throwable) {
        final CheckpointException checkpointException =
                getCheckpointException(
                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);
        onCompletionPromise.completeExceptionally(checkpointException);
        onTriggerFailure((PendingCheckpoint) null, onCompletionPromise.props, checkpointException);
    }

    private void onTriggerFailure(PendingCheckpoint checkpoint, Throwable throwable) {
        checkArgument(checkpoint != null, "Pending checkpoint can not be null.");

        onTriggerFailure(checkpoint, checkpoint.getProps(), throwable);
    }

    /**
     * The trigger request is failed. NOTE, it must be invoked if trigger request is failed.
     *
     * @param checkpoint the pending checkpoint which is failed. It could be null if it's failed
     *     prematurely without a proper initialization.
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(
            @Nullable PendingCheckpoint checkpoint,
            CheckpointProperties checkpointProperties,
            Throwable throwable) {
        // beautify the stack trace a bit
        throwable = ExceptionUtils.stripCompletionException(throwable);

        try {
            coordinatorsToCheckpoint.forEach(
                    OperatorCoordinatorCheckpointContext::abortCurrentTriggering);

            final CheckpointException cause =
                    getCheckpointException(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);

            if (checkpoint != null && !checkpoint.isDisposed()) {
                synchronized (lock) {
                    abortPendingCheckpoint(checkpoint, cause);
                }
            } else {
                failureManager.handleCheckpointException(
                        checkpoint, checkpointProperties, cause, null, job, null, statsTracker);
            }
        } finally {
            isTriggering = false;
            executeQueuedRequest();
        }
    }

    private void executeQueuedRequest() {
        chooseQueuedRequestToExecute().ifPresent(this::startTriggeringCheckpoint);
    }

    private Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute() {
        synchronized (lock) {
            return requestDecider.chooseQueuedRequestToExecute(
                    isTriggering, lastCheckpointCompletionRelativeTime);
        }
    }

    private Optional<CheckpointTriggerRequest> chooseRequestToExecute(
            CheckpointTriggerRequest request) {
        synchronized (lock) {
            Optional<CheckpointTriggerRequest> checkpointTriggerRequest =
                    requestDecider.chooseRequestToExecute(
                            request, isTriggering, lastCheckpointCompletionRelativeTime);
            return checkpointTriggerRequest;
        }
    }

    // Returns true if the checkpoint is successfully completed, false otherwise.
    private boolean maybeCompleteCheckpoint(PendingCheckpoint checkpoint) {
        synchronized (lock) {
            if (checkpoint.isFullyAcknowledged()) {
                try {
                    // we need to check inside the lock for being shutdown as well,
                    // otherwise we get races and invalid error log messages.
                    if (shutdown) {
                        return false;
                    }
                    completePendingCheckpoint(checkpoint);
                } catch (CheckpointException ce) {
                    onTriggerFailure(checkpoint, ce);
                    return false;
                }
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------
    //  Handling checkpoints and messages
    // --------------------------------------------------------------------------------------------

    /**
     * Receives a {@link DeclineCheckpoint} message for a pending checkpoint.
     *
     * @param message Checkpoint decline from the task manager
     * @param taskManagerLocationInfo The location info of the decline checkpoint message's sender
     */
    public void receiveDeclineMessage(DeclineCheckpoint message, String taskManagerLocationInfo) {
        if (shutdown || message == null) {
            return;
        }

        if (!job.equals(message.getJob())) {
            throw new IllegalArgumentException(
                    "Received DeclineCheckpoint message for job "
                            + message.getJob()
                            + " from "
                            + taskManagerLocationInfo
                            + " while this coordinator handles job "
                            + job);
        }

        final long checkpointId = message.getCheckpointId();
        final CheckpointException checkpointException =
                message.getSerializedCheckpointException().unwrap();
        final String reason = checkpointException.getMessage();

        PendingCheckpoint checkpoint;

        synchronized (lock) {
            // we need to check inside the lock for being shutdown as well, otherwise we
            // get races and invalid error log messages
            if (shutdown) {
                return;
            }

            checkpoint = pendingCheckpoints.get(checkpointId);

            if (checkpoint != null) {
                Preconditions.checkState(
                        !checkpoint.isDisposed(),
                        "Received message for discarded but non-removed checkpoint "
                                + checkpointId);
                LOG.info(
                        "Decline checkpoint {} by task {} of job {} at {}.",
                        checkpointId,
                        message.getTaskExecutionId(),
                        job,
                        taskManagerLocationInfo,
                        checkpointException.getCause());
                abortPendingCheckpoint(
                        checkpoint, checkpointException, message.getTaskExecutionId());
            } else if (LOG.isDebugEnabled()) {
                if (recentExpiredCheckpoints.contains(checkpointId)) {
                    // message is for an expired checkpoint
                    LOG.debug(
                            "Received another decline message for now expired checkpoint attempt {} from task {} of job {} at {} : {}",
                            checkpointId,
                            message.getTaskExecutionId(),
                            job,
                            taskManagerLocationInfo,
                            reason);
                } else {
                    // message is for an unknown checkpoint. might be so old that we don't even
                    // remember it any more
                    LOG.debug(
                            "Received decline message for unknown (too old?) checkpoint attempt {} from task {} of job {} at {} : {}",
                            checkpointId,
                            message.getTaskExecutionId(),
                            job,
                            taskManagerLocationInfo,
                            reason);
                }
            }
        }
    }

    /**
     * Receives an AcknowledgeCheckpoint message and returns whether the message was associated with
     * a pending checkpoint.
     *
     * @param message Checkpoint ack from the task manager
     * @param taskManagerLocationInfo The location of the acknowledge checkpoint message's sender
     * @return Flag indicating whether the ack'd checkpoint was associated with a pending
     *     checkpoint.
     * @throws CheckpointException If the checkpoint cannot be added to the completed checkpoint
     *     store.
     */
    public boolean receiveAcknowledgeMessage(
            AcknowledgeCheckpoint message, String taskManagerLocationInfo)
            throws CheckpointException {
        if (shutdown || message == null) {
            return false;
        }

        if (!job.equals(message.getJob())) {
            LOG.error(
                    "Received wrong AcknowledgeCheckpoint message for job {} from {} : {}",
                    job,
                    taskManagerLocationInfo,
                    message);
            return false;
        }

        final long checkpointId = message.getCheckpointId();

        synchronized (lock) {
            // we need to check inside the lock for being shutdown as well, otherwise we
            // get races and invalid error log messages
            if (shutdown) {
                return false;
            }

            final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

            if (message.getSubtaskState() != null) {
                // Register shared state regardless of checkpoint state and task ACK state.
                // This way, shared state is
                // 1. kept if the message is late or state will be used by the task otherwise
                // 2. removed eventually upon checkpoint subsumption (or job cancellation)
                // Do not register savepoints' shared state, as Flink is not in charge of
                // savepoints' lifecycle
                if (checkpoint == null || !checkpoint.getProps().isSavepoint()) {
                    message.getSubtaskState()
                            .registerSharedStates(
                                    completedCheckpointStore.getSharedStateRegistry(),
                                    checkpointId);
                }
            }

            if (checkpoint != null && !checkpoint.isDisposed()) {

                switch (checkpoint.acknowledgeTask(
                        message.getTaskExecutionId(),
                        message.getSubtaskState(),
                        message.getCheckpointMetrics())) {
                    case SUCCESS:
                        LOG.debug(
                                "Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
                                checkpointId,
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        if (checkpoint.isFullyAcknowledged()) {
                            completePendingCheckpoint(checkpoint);
                        }
                        break;
                    case DUPLICATE:
                        LOG.debug(
                                "Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);
                        break;
                    case UNKNOWN:
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the task's execution attempt id was unknown. Discarding "
                                        + "the state handle to avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());

                        break;
                    case DISCARDED:
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the pending checkpoint had been discarded. Discarding the "
                                        + "state handle tp avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());
                }

                return true;
            } else if (checkpoint != null) {
                // this should not happen
                throw new IllegalStateException(
                        "Received message for discarded but non-removed checkpoint "
                                + checkpointId);
            } else {
                reportStats(
                        message.getCheckpointId(),
                        message.getTaskExecutionId(),
                        message.getCheckpointMetrics());
                boolean wasPendingCheckpoint;

                // message is for an unknown checkpoint, or comes too late (checkpoint disposed)
                if (recentExpiredCheckpoints.contains(checkpointId)) {
                    wasPendingCheckpoint = true;
                    LOG.warn(
                            "Received late message for now expired checkpoint attempt {} from task "
                                    + "{} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                } else {
                    LOG.debug(
                            "Received message for an unknown checkpoint {} from task {} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                    wasPendingCheckpoint = false;
                }

                // try to discard the state so that we don't have lingering state lying around
                discardSubtaskState(
                        message.getJob(),
                        message.getTaskExecutionId(),
                        message.getCheckpointId(),
                        message.getSubtaskState());

                return wasPendingCheckpoint;
            }
        }
    }

    /**
     * Try to complete the given pending checkpoint.
     *
     * <p>Important: This method should only be called in the checkpoint lock scope.
     *
     * @param pendingCheckpoint to complete
     * @throws CheckpointException if the completion failed
     */
    private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        final long checkpointId = pendingCheckpoint.getCheckpointID();
        final CompletedCheckpoint completedCheckpoint;
        final CompletedCheckpoint lastSubsumed;
        final CheckpointProperties props = pendingCheckpoint.getProps();

        completedCheckpointStore.getSharedStateRegistry().checkpointCompleted(checkpointId);

        try {
            completedCheckpoint = finalizeCheckpoint(pendingCheckpoint);

            // the pending checkpoint must be discarded after the finalization
            Preconditions.checkState(pendingCheckpoint.isDisposed() && completedCheckpoint != null);

            if (!props.isSavepoint()) {
                lastSubsumed =
                        addCompletedCheckpointToStoreAndSubsumeOldest(
                                checkpointId, completedCheckpoint, pendingCheckpoint);
            } else {
                lastSubsumed = null;
            }

            pendingCheckpoint.getCompletionFuture().complete(completedCheckpoint);
            reportCompletedCheckpoint(completedCheckpoint);
        } catch (Exception exception) {
            // For robustness reasons, we need catch exception and try marking the checkpoint
            // completed.
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            throw exception;
        } finally {
            pendingCheckpoints.remove(checkpointId);
            scheduleTriggerRequest();
        }

        cleanupAfterCompletedCheckpoint(
                pendingCheckpoint, checkpointId, completedCheckpoint, lastSubsumed, props);
    }

    private void reportCompletedCheckpoint(CompletedCheckpoint completedCheckpoint) {
        failureManager.handleCheckpointSuccess(completedCheckpoint.getCheckpointID());
        CompletedCheckpointStats completedCheckpointStats = completedCheckpoint.getStatistic();
        if (completedCheckpointStats != null) {
            LOG.trace(
                    "Checkpoint {} size: {}Kb, duration: {}ms",
                    completedCheckpoint.getCheckpointID(),
                    completedCheckpointStats.getStateSize() == 0
                            ? 0
                            : completedCheckpointStats.getStateSize() / 1024,
                    completedCheckpointStats.getEndToEndDuration());
            // Finalize the statsCallback and give the completed checkpoint a
            // callback for discards.
            statsTracker.reportCompletedCheckpoint(completedCheckpointStats);
        }
    }

    private void cleanupAfterCompletedCheckpoint(
            PendingCheckpoint pendingCheckpoint,
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            CompletedCheckpoint lastSubsumed,
            CheckpointProperties props) {

        // record the time when this was completed, to calculate
        // the 'min delay between checkpoints'
        lastCheckpointCompletionRelativeTime = clock.relativeTimeMillis();

        logCheckpointInfo(completedCheckpoint);

        if (!props.isSavepoint() || props.isSynchronous()) {
            // drop those pending checkpoints that are at prior to the completed one
            dropSubsumedCheckpoints(checkpointId);

            // send the "notify complete" call to all vertices, coordinators, etc.
            sendAcknowledgeMessages(
                    pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo(),
                    checkpointId,
                    completedCheckpoint.getTimestamp(),
                    extractIdIfDiscardedOnSubsumed(lastSubsumed));
        }
    }

    private void logCheckpointInfo(CompletedCheckpoint completedCheckpoint) {
        LOG.info(
                "Completed checkpoint {} for job {} ({} bytes, checkpointDuration={} ms, finalizationTime={} ms).",
                completedCheckpoint.getCheckpointID(),
                job,
                completedCheckpoint.getStateSize(),
                completedCheckpoint.getCompletionTimestamp() - completedCheckpoint.getTimestamp(),
                System.currentTimeMillis() - completedCheckpoint.getCompletionTimestamp());

        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Checkpoint state: ");
            for (OperatorState state : completedCheckpoint.getOperatorStates().values()) {
                builder.append(state);
                builder.append(", ");
            }
            // Remove last two chars ", "
            builder.setLength(builder.length() - 2);

            LOG.debug(builder.toString());
        }
    }

    private CompletedCheckpoint finalizeCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        try {
            final CompletedCheckpoint completedCheckpoint =
                    pendingCheckpoint.finalizeCheckpoint(
                            checkpointsCleaner, this::scheduleTriggerRequest, executor);

            return completedCheckpoint;
        } catch (Exception e1) {
            // abort the current pending checkpoint if we fails to finalize the pending
            // checkpoint.
            final CheckpointFailureReason failureReason =
                    e1 instanceof PartialFinishingNotSupportedByStateException
                            ? CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING
                            : CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE;

            if (!pendingCheckpoint.isDisposed()) {
                abortPendingCheckpoint(
                        pendingCheckpoint, new CheckpointException(failureReason, e1));
            }

            throw new CheckpointException(
                    "Could not finalize the pending checkpoint "
                            + pendingCheckpoint.getCheckpointID()
                            + '.',
                    failureReason,
                    e1);
        }
    }

    private long extractIdIfDiscardedOnSubsumed(CompletedCheckpoint lastSubsumed) {
        final long lastSubsumedCheckpointId;
        if (lastSubsumed != null && lastSubsumed.getProperties().discardOnSubsumed()) {
            lastSubsumedCheckpointId = lastSubsumed.getCheckpointID();
        } else {
            lastSubsumedCheckpointId = CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
        }
        return lastSubsumedCheckpointId;
    }

    private CompletedCheckpoint addCompletedCheckpointToStoreAndSubsumeOldest(
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        List<ExecutionVertex> tasksToAbort =
                pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo();
        try {
            final CompletedCheckpoint subsumedCheckpoint =
                    completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                            completedCheckpoint, checkpointsCleaner, this::scheduleTriggerRequest);
            // reset the force full snapshot flag, we should've completed at least one full
            // snapshot by now
            this.forceFullSnapshot = false;
            return subsumedCheckpoint;
        } catch (Exception exception) {
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            if (exception instanceof PossibleInconsistentStateException) {
                LOG.warn(
                        "An error occurred while writing checkpoint {} to the underlying metadata"
                                + " store. Flink was not able to determine whether the metadata was"
                                + " successfully persisted. The corresponding state located at '{}'"
                                + " won't be discarded and needs to be cleaned up manually.",
                        completedCheckpoint.getCheckpointID(),
                        completedCheckpoint.getExternalPointer());
            } else {
                // we failed to store the completed checkpoint. Let's clean up
                checkpointsCleaner.cleanCheckpointOnFailedStoring(completedCheckpoint, executor);
            }

            final CheckpointException checkpointException =
                    new CheckpointException(
                            "Could not complete the pending checkpoint " + checkpointId + '.',
                            CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE,
                            exception);
            reportFailedCheckpoint(pendingCheckpoint, checkpointException);
            sendAbortedMessages(tasksToAbort, checkpointId, completedCheckpoint.getTimestamp());
            throw checkpointException;
        }
    }

    private void reportFailedCheckpoint(
            PendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        failureManager.handleCheckpointException(
                pendingCheckpoint,
                pendingCheckpoint.getProps(),
                exception,
                null,
                job,
                getStatsCallback(pendingCheckpoint),
                statsTracker);
    }

    void scheduleTriggerRequest() {
        synchronized (lock) {
            if (isShutdown()) {
                LOG.debug(
                        "Skip scheduling trigger request because the CheckpointCoordinator is shut down");
            } else {
                timer.execute(this::executeQueuedRequest);
            }
        }
    }

    @VisibleForTesting
    void sendAcknowledgeMessages(
            List<ExecutionVertex> tasksToCommit,
            long completedCheckpointId,
            long completedTimestamp,
            long lastSubsumedCheckpointId) {
        // commit tasks
        for (ExecutionVertex ev : tasksToCommit) {
            Execution ee = ev.getCurrentExecutionAttempt();
            if (ee != null) {
                ee.notifyCheckpointOnComplete(
                        completedCheckpointId, completedTimestamp, lastSubsumedCheckpointId);
            }
        }

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext : coordinatorsToCheckpoint) {
            coordinatorContext.notifyCheckpointComplete(completedCheckpointId);
        }
    }

    private void sendAbortedMessages(
            List<ExecutionVertex> tasksToAbort, long checkpointId, long timeStamp) {
        assert (Thread.holdsLock(lock));
        long latestCompletedCheckpointId = completedCheckpointStore.getLatestCheckpointId();

        // send notification of aborted checkpoints asynchronously.
        executor.execute(
                () -> {
                    // send the "abort checkpoint" messages to necessary vertices.
                    for (ExecutionVertex ev : tasksToAbort) {
                        Execution ee = ev.getCurrentExecutionAttempt();
                        if (ee != null) {
                            ee.notifyCheckpointAborted(
                                    checkpointId, latestCompletedCheckpointId, timeStamp);
                        }
                    }
                });

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext : coordinatorsToCheckpoint) {
            coordinatorContext.notifyCheckpointAborted(checkpointId);
        }
    }

    /**
     * Fails all pending checkpoints which have not been acknowledged by the given execution attempt
     * id.
     *
     * @param executionAttemptId for which to discard unacknowledged pending checkpoints
     * @param cause of the failure
     */
    public void failUnacknowledgedPendingCheckpointsFor(
            ExecutionAttemptID executionAttemptId, Throwable cause) {
        synchronized (lock) {
            abortPendingCheckpoints(
                    checkpoint -> !checkpoint.isAcknowledgedBy(executionAttemptId),
                    new CheckpointException(CheckpointFailureReason.TASK_FAILURE, cause));
        }
    }

    private void rememberRecentExpiredCheckpointId(long id) {
        if (recentExpiredCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
            recentExpiredCheckpoints.removeFirst();
        }
        recentExpiredCheckpoints.addLast(id);
    }

    private void dropSubsumedCheckpoints(long checkpointId) {
        abortPendingCheckpoints(
                checkpoint ->
                        checkpoint.getCheckpointID() < checkpointId && checkpoint.canBeSubsumed(),
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_SUBSUMED));
    }

    // --------------------------------------------------------------------------------------------
    //  Checkpoint State Restoring
    // --------------------------------------------------------------------------------------------

    /**
     * Restores the latest checkpointed state to a set of subtasks. This method represents a "local"
     * or "regional" failover and does restore states to coordinators. Note that a regional failover
     * might still include all tasks.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @return An {@code OptionalLong} with the checkpoint ID, if state was restored, an empty
     *     {@code OptionalLong} otherwise.
     * @throws IllegalStateException If the CheckpointCoordinator is shut down.
     * @throws IllegalStateException If no completed checkpoint is available and the <code>
     *     failIfNoCheckpoint</code> flag has been set.
     * @throws IllegalStateException If the checkpoint contains state that cannot be mapped to any
     *     job vertex in <code>tasks</code> and the <code>allowNonRestoredState</code> flag has not
     *     been set.
     * @throws IllegalStateException If the max parallelism changed for an operator that restores
     *     state from this checkpoint.
     * @throws IllegalStateException If the parallelism changed for an operator that restores
     *     <i>non-partitioned</i> state from this checkpoint.
     */
    public OptionalLong restoreLatestCheckpointedStateToSubtasks(
            final Set<ExecutionJobVertex> tasks) throws Exception {
        // when restoring subtasks only we accept potentially unmatched state for the
        // following reasons
        //   - the set frequently does not include all Job Vertices (only the ones that are part
        //     of the restarted region), meaning there will be unmatched state by design.
        //   - because what we might end up restoring from an original savepoint with unmatched
        //     state, if there is was no checkpoint yet.
        return restoreLatestCheckpointedStateInternal(
                tasks,
                OperatorCoordinatorRestoreBehavior
                        .SKIP, // local/regional recovery does not reset coordinators
                false, // recovery might come before first successful checkpoint
                true,
                false); // see explanation above
    }

    /**
     * Restores the latest checkpointed state to all tasks and all coordinators. This method
     * represents a "global restore"-style operation where all stateful tasks and coordinators from
     * the given set of Job Vertices are restored. are restored to their latest checkpointed state.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @param allowNonRestoredState Allow checkpoint state that cannot be mapped to any job vertex
     *     in tasks.
     * @return <code>true</code> if state was restored, <code>false</code> otherwise.
     * @throws IllegalStateException If the CheckpointCoordinator is shut down.
     * @throws IllegalStateException If no completed checkpoint is available and the <code>
     *     failIfNoCheckpoint</code> flag has been set.
     * @throws IllegalStateException If the checkpoint contains state that cannot be mapped to any
     *     job vertex in <code>tasks</code> and the <code>allowNonRestoredState</code> flag has not
     *     been set.
     * @throws IllegalStateException If the max parallelism changed for an operator that restores
     *     state from this checkpoint.
     * @throws IllegalStateException If the parallelism changed for an operator that restores
     *     <i>non-partitioned</i> state from this checkpoint.
     */
    public boolean restoreLatestCheckpointedStateToAll(
            final Set<ExecutionJobVertex> tasks, final boolean allowNonRestoredState)
            throws Exception {

        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        tasks,
                        OperatorCoordinatorRestoreBehavior
                                .RESTORE_OR_RESET, // global recovery restores coordinators, or
                        // resets them to empty
                        false, // recovery might come before first successful checkpoint
                        allowNonRestoredState,
                        false);

        return restoredCheckpointId.isPresent();
    }

    /**
     * Restores the latest checkpointed at the beginning of the job execution. If there is a
     * checkpoint, this method acts like a "global restore"-style operation where all stateful tasks
     * and coordinators from the given set of Job Vertices are restored.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @return True, if a checkpoint was found and its state was restored, false otherwise.
     */
    public boolean restoreInitialCheckpointIfPresent(final Set<ExecutionJobVertex> tasks)
            throws Exception {
        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        tasks,
                        OperatorCoordinatorRestoreBehavior.RESTORE_IF_CHECKPOINT_PRESENT,
                        false, // initial checkpoints exist only on JobManager failover. ok if not
                        // present.
                        false,
                        true); // JobManager failover means JobGraphs match exactly.

        return restoredCheckpointId.isPresent();
    }

    /**
     * Performs the actual restore operation to the given tasks.
     *
     * <p>This method returns the restored checkpoint ID (as an optional) or an empty optional, if
     * no checkpoint was restored.
     */
    private OptionalLong restoreLatestCheckpointedStateInternal(
            final Set<ExecutionJobVertex> tasks,
            final OperatorCoordinatorRestoreBehavior operatorCoordinatorRestoreBehavior,
            final boolean errorIfNoCheckpoint,
            final boolean allowNonRestoredState,
            final boolean checkForPartiallyFinishedOperators)
            throws Exception {

        synchronized (lock) {
            if (shutdown) {
                throw new IllegalStateException("CheckpointCoordinator is shut down");
            }

            // Restore from the latest checkpoint
            CompletedCheckpoint latest = completedCheckpointStore.getLatestCheckpoint();

            if (latest == null) {
                LOG.info("No checkpoint found during restore.");

                if (errorIfNoCheckpoint) {
                    throw new IllegalStateException("No completed checkpoint available");
                }

                LOG.debug("Resetting the master hooks.");
                MasterHooks.reset(masterHooks.values(), LOG);

                if (operatorCoordinatorRestoreBehavior
                        == OperatorCoordinatorRestoreBehavior.RESTORE_OR_RESET) {
                    // we let the JobManager-side components know that there was a recovery,
                    // even if there was no checkpoint to recover from, yet
                    LOG.info("Resetting the Operator Coordinators to an empty state.");
                    restoreStateToCoordinators(
                            OperatorCoordinator.NO_CHECKPOINT, Collections.emptyMap());
                }

                return OptionalLong.empty();
            }

            LOG.info("Restoring job {} from {}.", job, latest);

            this.forceFullSnapshot = latest.getProperties().isUnclaimed();

            // re-assign the task states
            final Map<OperatorID, OperatorState> operatorStates = extractOperatorStates(latest);

            if (checkForPartiallyFinishedOperators) {
                VertexFinishedStateChecker vertexFinishedStateChecker =
                        vertexFinishedStateCheckerFactory.apply(tasks, operatorStates);
                vertexFinishedStateChecker.validateOperatorsFinishedState();
            }

            StateAssignmentOperation stateAssignmentOperation =
                    new StateAssignmentOperation(
                            latest.getCheckpointID(), tasks, operatorStates, allowNonRestoredState);

            stateAssignmentOperation.assignStates();

            // call master hooks for restore. we currently call them also on "regional restore"
            // because
            // there is no other failure notification mechanism in the master hooks
            // ultimately these should get removed anyways in favor of the operator coordinators

            MasterHooks.restoreMasterHooks(
                    masterHooks,
                    latest.getMasterHookStates(),
                    latest.getCheckpointID(),
                    allowNonRestoredState,
                    LOG);

            if (operatorCoordinatorRestoreBehavior != OperatorCoordinatorRestoreBehavior.SKIP) {
                restoreStateToCoordinators(latest.getCheckpointID(), operatorStates);
            }

            // update metrics

            long restoreTimestamp = System.currentTimeMillis();
            RestoredCheckpointStats restored =
                    new RestoredCheckpointStats(
                            latest.getCheckpointID(),
                            latest.getProperties(),
                            restoreTimestamp,
                            latest.getExternalPointer());

            statsTracker.reportRestoredCheckpoint(restored);

            return OptionalLong.of(latest.getCheckpointID());
        }
    }

    private Map<OperatorID, OperatorState> extractOperatorStates(CompletedCheckpoint checkpoint) {
        Map<OperatorID, OperatorState> originalOperatorStates = checkpoint.getOperatorStates();

        if (checkpoint.getCheckpointID() != checkpointIdOfIgnoredInFlightData) {
            // Don't do any changes if it is not required.
            return originalOperatorStates;
        }

        HashMap<OperatorID, OperatorState> newStates = new HashMap<>();
        // Create the new operator states without in-flight data.
        for (OperatorState originalOperatorState : originalOperatorStates.values()) {
            newStates.put(
                    originalOperatorState.getOperatorID(),
                    originalOperatorState.copyAndDiscardInFlightData());
        }

        return newStates;
    }

    /**
     * Restore the state with given savepoint.
     *
     * @param restoreSettings Settings for a snapshot to restore from. Includes the path and
     *     parameters for the restore process.
     * @param tasks Map of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @param userClassLoader The class loader to resolve serialized classes in legacy savepoint
     *     versions.
     */
    public boolean restoreSavepoint(
            SavepointRestoreSettings restoreSettings,
            Map<JobVertexID, ExecutionJobVertex> tasks,
            ClassLoader userClassLoader)
            throws Exception {

        final String savepointPointer = restoreSettings.getRestorePath();
        final boolean allowNonRestored = restoreSettings.allowNonRestoredState();
        Preconditions.checkNotNull(savepointPointer, "The savepoint path cannot be null.");

        LOG.info(
                "Starting job {} from savepoint {} ({})",
                job,
                savepointPointer,
                (allowNonRestored ? "allowing non restored state" : ""));

        final CompletedCheckpointStorageLocation checkpointLocation =
                checkpointStorageView.resolveCheckpoint(savepointPointer);

        // convert to checkpoint so the system can fall back to it
        final CheckpointProperties checkpointProperties;
        switch (restoreSettings.getRestoreMode()) {
            case CLAIM:
                checkpointProperties = this.checkpointProperties;
                break;
            case LEGACY:
                checkpointProperties =
                        CheckpointProperties.forSavepoint(
                                false,
                                // we do not care about the format when restoring, the format is
                                // necessary when triggering a savepoint
                                SavepointFormatType.CANONICAL);
                break;
            case NO_CLAIM:
                checkpointProperties = CheckpointProperties.forUnclaimedSnapshot();
                break;
            default:
                throw new IllegalArgumentException("Unknown snapshot restore mode");
        }

        // Load the savepoint as a checkpoint into the system
        CompletedCheckpoint savepoint =
                Checkpoints.loadAndValidateCheckpoint(
                        job,
                        tasks,
                        checkpointLocation,
                        userClassLoader,
                        allowNonRestored,
                        checkpointProperties);

        // register shared state - even before adding the checkpoint to the store
        // because the latter might trigger subsumption so the ref counts must be up-to-date
        savepoint.registerSharedStatesAfterRestored(
                completedCheckpointStore.getSharedStateRegistry(),
                restoreSettings.getRestoreMode());

        completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                savepoint, checkpointsCleaner, this::scheduleTriggerRequest);

        // Reset the checkpoint ID counter
        long nextCheckpointId = savepoint.getCheckpointID() + 1;
        checkpointIdCounter.setCount(nextCheckpointId);

        LOG.info("Reset the checkpoint ID of job {} to {}.", job, nextCheckpointId);

        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        new HashSet<>(tasks.values()),
                        OperatorCoordinatorRestoreBehavior.RESTORE_IF_CHECKPOINT_PRESENT,
                        true,
                        allowNonRestored,
                        true);

        return restoredCheckpointId.isPresent();
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    public int getNumberOfPendingCheckpoints() {
        synchronized (lock) {
            return this.pendingCheckpoints.size();
        }
    }

    public int getNumberOfRetainedSuccessfulCheckpoints() {
        synchronized (lock) {
            return completedCheckpointStore.getNumberOfRetainedCheckpoints();
        }
    }

    public Map<Long, PendingCheckpoint> getPendingCheckpoints() {
        synchronized (lock) {
            return new HashMap<>(this.pendingCheckpoints);
        }
    }

    public List<CompletedCheckpoint> getSuccessfulCheckpoints() throws Exception {
        synchronized (lock) {
            return completedCheckpointStore.getAllCheckpoints();
        }
    }

    @VisibleForTesting
    public ArrayDeque<Long> getRecentExpiredCheckpoints() {
        return recentExpiredCheckpoints;
    }

    public CheckpointStorageCoordinatorView getCheckpointStorage() {
        return checkpointStorageView;
    }

    public CompletedCheckpointStore getCheckpointStore() {
        return completedCheckpointStore;
    }

    /**
     * Gets the checkpoint interval. Its value might vary depending on whether there is processing
     * backlog.
     */
    private long getCurrentCheckpointInterval() {
        return backlogOperators.isEmpty() ? baseInterval : baseIntervalDuringBacklog;
    }

    public long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    /** @deprecated use {@link #getNumQueuedRequests()} */
    @Deprecated
    @VisibleForTesting
    PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
        synchronized (lock) {
            return requestDecider.getTriggerRequestQueue();
        }
    }

    public boolean isTriggering() {
        return isTriggering;
    }

    @VisibleForTesting
    boolean isCurrentPeriodicTriggerAvailable() {
        return currentPeriodicTrigger != null;
    }

    /**
     * Returns whether periodic checkpointing has been configured.
     *
     * @return <code>true</code> if periodic checkpoints have been configured.
     */
    public boolean isPeriodicCheckpointingConfigured() {
        return baseInterval != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
    }

    // --------------------------------------------------------------------------------------------
    //  Periodic scheduling of checkpoints
    // --------------------------------------------------------------------------------------------

    public void startCheckpointScheduler() {
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalArgumentException("Checkpoint coordinator is shut down");
            }
            Preconditions.checkState(
                    isPeriodicCheckpointingConfigured(),
                    "Can not start checkpoint scheduler, if no periodic checkpointing is configured");

            // make sure all prior timers are cancelled
            stopCheckpointScheduler();

            periodicScheduling = true;
            scheduleTriggerWithDelay(clock.relativeTimeMillis(), getRandomInitDelay());
        }
    }

    public void stopCheckpointScheduler() {
        synchronized (lock) {
            periodicScheduling = false;

            cancelPeriodicTrigger();

            final CheckpointException reason =
                    new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SUSPEND);
            abortPendingAndQueuedCheckpoints(reason);
        }
    }

    public boolean isPeriodicCheckpointingStarted() {
        return periodicScheduling;
    }

    /**
     * Aborts all the pending checkpoints due to en exception.
     *
     * @param exception The exception.
     */
    public void abortPendingCheckpoints(CheckpointException exception) {
        synchronized (lock) {
            abortPendingCheckpoints(ignored -> true, exception);
        }
    }

    private void abortPendingCheckpoints(
            Predicate<PendingCheckpoint> checkpointToFailPredicate, CheckpointException exception) {

        assert Thread.holdsLock(lock);

        final PendingCheckpoint[] pendingCheckpointsToFail =
                pendingCheckpoints.values().stream()
                        .filter(checkpointToFailPredicate)
                        .toArray(PendingCheckpoint[]::new);

        // do not traverse pendingCheckpoints directly, because it might be changed during
        // traversing
        for (PendingCheckpoint pendingCheckpoint : pendingCheckpointsToFail) {
            abortPendingCheckpoint(pendingCheckpoint, exception);
        }
    }

    private void rescheduleTrigger(long currentTimeMillis, long tillNextMillis) {
        cancelPeriodicTrigger();
        scheduleTriggerWithDelay(currentTimeMillis, tillNextMillis);
    }

    private void cancelPeriodicTrigger() {
        if (currentPeriodicTrigger != null) {
            nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
            currentPeriodicTriggerFuture.cancel(false);
            currentPeriodicTrigger = null;
            currentPeriodicTriggerFuture = null;
        }
    }

    private long getRandomInitDelay() {
        return ThreadLocalRandom.current().nextLong(minPauseBetweenCheckpoints, baseInterval + 1L);
    }

    private void scheduleTriggerWithDelay(long currentRelativeTime, long initDelay) {
        nextCheckpointTriggeringRelativeTime = currentRelativeTime + initDelay;
        currentPeriodicTrigger = new ScheduledTrigger();
        currentPeriodicTriggerFuture =
                timer.schedule(currentPeriodicTrigger, initDelay, TimeUnit.MILLISECONDS);
    }

    private void restoreStateToCoordinators(
            final long checkpointId, final Map<OperatorID, OperatorState> operatorStates)
            throws Exception {

        for (OperatorCoordinatorCheckpointContext coordContext : coordinatorsToCheckpoint) {
            final OperatorState state = operatorStates.get(coordContext.operatorId());
            final ByteStreamStateHandle coordinatorState =
                    state == null ? null : state.getCoordinatorState();
            final byte[] bytes = coordinatorState == null ? null : coordinatorState.getData();
            coordContext.resetToCheckpoint(checkpointId, bytes);
        }
    }

    // ------------------------------------------------------------------------
    //  job status listener that schedules / cancels periodic checkpoints
    // ------------------------------------------------------------------------

    public JobStatusListener createActivatorDeactivator() {
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalArgumentException("Checkpoint coordinator is shut down");
            }

            if (jobStatusListener == null) {
                jobStatusListener = new CheckpointCoordinatorDeActivator(this);
            }

            return jobStatusListener;
        }
    }

    int getNumQueuedRequests() {
        synchronized (lock) {
            return requestDecider.getNumQueuedRequests();
        }
    }

    public void reportStats(long id, ExecutionAttemptID attemptId, CheckpointMetrics metrics) {
        statsTracker.reportIncompleteStats(id, attemptId, metrics);
    }

    // ------------------------------------------------------------------------

    final class ScheduledTrigger implements Runnable {

        @Override
        public void run() {
            synchronized (lock) {
                if (currentPeriodicTrigger != this) {
                    // Another periodic trigger has been scheduled but this one
                    // has not been force cancelled yet.
                    return;
                }

                long checkpointInterval = getCurrentCheckpointInterval();
                if (checkpointInterval
                        != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL) {
                    nextCheckpointTriggeringRelativeTime += checkpointInterval;
                    currentPeriodicTriggerFuture =
                            timer.schedule(
                                    this,
                                    Math.max(
                                            0,
                                            nextCheckpointTriggeringRelativeTime
                                                    - clock.relativeTimeMillis()),
                                    TimeUnit.MILLISECONDS);
                } else {
                    nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
                    currentPeriodicTrigger = null;
                    currentPeriodicTriggerFuture = null;
                }
            }

            try {
                triggerCheckpoint(checkpointProperties, null, true);
            } catch (Exception e) {
                LOG.error("Exception while triggering checkpoint for job {}.", job, e);
            }
        }
    }

    /**
     * Discards the given state object asynchronously belonging to the given job, execution attempt
     * id and checkpoint id.
     *
     * @param jobId identifying the job to which the state object belongs
     * @param executionAttemptID identifying the task to which the state object belongs
     * @param checkpointId of the state object
     * @param subtaskState to discard asynchronously
     */
    private void discardSubtaskState(
            final JobID jobId,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final TaskStateSnapshot subtaskState) {

        if (subtaskState != null) {
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {

                            try {
                                subtaskState.discardState();
                            } catch (Throwable t2) {
                                LOG.warn(
                                        "Could not properly discard state object of checkpoint {} "
                                                + "belonging to task {} of job {}.",
                                        checkpointId,
                                        executionAttemptID,
                                        jobId,
                                        t2);
                            }
                        }
                    });
        }
    }

    private void abortPendingCheckpoint(
            PendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        abortPendingCheckpoint(pendingCheckpoint, exception, null);
    }

    private void abortPendingCheckpoint(
            PendingCheckpoint pendingCheckpoint,
            CheckpointException exception,
            @Nullable final ExecutionAttemptID executionAttemptID) {

        assert (Thread.holdsLock(lock));

        if (!pendingCheckpoint.isDisposed()) {
            try {
                // release resource here
                pendingCheckpoint.abort(
                        exception.getCheckpointFailureReason(),
                        exception.getCause(),
                        checkpointsCleaner,
                        this::scheduleTriggerRequest,
                        executor,
                        statsTracker);

                failureManager.handleCheckpointException(
                        pendingCheckpoint,
                        pendingCheckpoint.getProps(),
                        exception,
                        executionAttemptID,
                        job,
                        getStatsCallback(pendingCheckpoint),
                        statsTracker);
            } finally {
                sendAbortedMessages(
                        pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo(),
                        pendingCheckpoint.getCheckpointID(),
                        pendingCheckpoint.getCheckpointTimestamp());
                pendingCheckpoints.remove(pendingCheckpoint.getCheckpointID());
                if (exception
                        .getCheckpointFailureReason()
                        .equals(CheckpointFailureReason.CHECKPOINT_EXPIRED)) {
                    rememberRecentExpiredCheckpointId(pendingCheckpoint.getCheckpointID());
                }
                scheduleTriggerRequest();
            }
        }
    }

    private void preCheckGlobalState(boolean isPeriodic) throws CheckpointException {
        // abort if the coordinator has been shutdown in the meantime
        if (shutdown) {
            throw new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
        }

        // Don't allow periodic checkpoint if scheduling has been disabled
        if (isPeriodic && !periodicScheduling) {
            throw new CheckpointException(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN);
        }
    }

    private void abortPendingAndQueuedCheckpoints(CheckpointException exception) {
        assert (Thread.holdsLock(lock));
        requestDecider.abortAll(exception);
        abortPendingCheckpoints(exception);
    }

    /**
     * The canceller of checkpoint. The checkpoint might be cancelled if it doesn't finish in a
     * configured period.
     */
    class CheckpointCanceller implements Runnable {

        private final PendingCheckpoint pendingCheckpoint;

        private CheckpointCanceller(PendingCheckpoint pendingCheckpoint) {
            this.pendingCheckpoint = checkNotNull(pendingCheckpoint);
        }

        @Override
        public void run() {
            synchronized (lock) {
                // only do the work if the checkpoint is not discarded anyways
                // note that checkpoint completion discards the pending checkpoint object
                if (!pendingCheckpoint.isDisposed()) {
                    LOG.info(
                            "Checkpoint {} of job {} expired before completing.",
                            pendingCheckpoint.getCheckpointID(),
                            job);

                    abortPendingCheckpoint(
                            pendingCheckpoint,
                            new CheckpointException(CheckpointFailureReason.CHECKPOINT_EXPIRED));
                }
            }
        }
    }

    private static CheckpointException getCheckpointException(
            CheckpointFailureReason defaultReason, Throwable throwable) {

        final Optional<IOException> ioExceptionOptional =
                findThrowable(throwable, IOException.class);
        if (ioExceptionOptional.isPresent()) {
            return new CheckpointException(CheckpointFailureReason.IO_EXCEPTION, throwable);
        } else {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    findThrowable(throwable, CheckpointException.class);
            return checkpointExceptionOptional.orElseGet(
                    () -> new CheckpointException(defaultReason, throwable));
        }
    }

    static class CheckpointTriggerRequest {
        final long timestamp;
        final CheckpointProperties props;
        final @Nullable String externalSavepointLocation;
        final boolean isPeriodic;
        private final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                new CompletableFuture<>();

        CheckpointTriggerRequest(
                CheckpointProperties props,
                @Nullable String externalSavepointLocation,
                boolean isPeriodic) {

            this.timestamp = System.currentTimeMillis();
            this.props = checkNotNull(props);
            this.externalSavepointLocation = externalSavepointLocation;
            this.isPeriodic = isPeriodic;
        }

        CompletableFuture<CompletedCheckpoint> getOnCompletionFuture() {
            return onCompletionPromise;
        }

        public void completeExceptionally(CheckpointException exception) {
            onCompletionPromise.completeExceptionally(exception);
        }

        public boolean isForce() {
            return props.forceCheckpoint();
        }
    }

    private enum OperatorCoordinatorRestoreBehavior {

        /** Coordinators are always restored. If there is no checkpoint, they are restored empty. */
        RESTORE_OR_RESET,

        /** Coordinators are restored if there was a checkpoint. */
        RESTORE_IF_CHECKPOINT_PRESENT,

        /** Coordinators are not restored during this checkpoint restore. */
        SKIP;
    }

    private PendingCheckpointStats trackPendingCheckpointStats(
            long checkpointId,
            CheckpointPlan checkpointPlan,
            CheckpointProperties props,
            long checkpointTimestamp) {
        Map<JobVertexID, Integer> vertices =
                Stream.concat(
                                checkpointPlan.getTasksToWaitFor().stream(),
                                checkpointPlan.getFinishedTasks().stream())
                        .map(Execution::getVertex)
                        .map(ExecutionVertex::getJobVertex)
                        .distinct()
                        .collect(
                                toMap(
                                        ExecutionJobVertex::getJobVertexId,
                                        ExecutionJobVertex::getParallelism));

        PendingCheckpointStats pendingCheckpointStats =
                statsTracker.reportPendingCheckpoint(
                        checkpointId, checkpointTimestamp, props, vertices);

        reportFinishedTasks(pendingCheckpointStats, checkpointPlan.getFinishedTasks());

        return pendingCheckpointStats;
    }

    private void reportFinishedTasks(
            PendingCheckpointStats pendingCheckpointStats, List<Execution> finishedTasks) {
        long now = System.currentTimeMillis();
        finishedTasks.forEach(
                execution ->
                        pendingCheckpointStats.reportSubtaskStats(
                                execution.getVertex().getJobvertexId(),
                                new SubtaskStateStats(execution.getParallelSubtaskIndex(), now)));
    }

    @Nullable
    private PendingCheckpointStats getStatsCallback(PendingCheckpoint pendingCheckpoint) {
        return statsTracker.getPendingCheckpointStats(pendingCheckpoint.getCheckpointID());
    }
}

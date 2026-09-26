/*
 * Licensed to the Apache Software Foundation (ASF)
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
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.state.RegionalCheckpointInfo;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Encapsulates regional checkpoint logic for {@link CheckpointCoordinator}, per FLIP-600.
 *
 * <p>This handler owns the mutable regional checkpoint state (consecutive counter, force-global
 * flag, region ID provider, all-sources-finished checker) and implements the core evaluation
 * ({@link #tryCompleteRegionalCheckpoint}), completion ({@link #completeRegionalCheckpoint}), and
 * state recombination ({@link #referenceFallbackState}) logic.
 *
 * <p>Thread-safety: all methods assume the caller holds the {@code CheckpointCoordinator}'s {@code
 * lock} (asserted via {@code Thread.holdsLock}).
 */
class RegionalCheckpointHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RegionalCheckpointHandler.class);

    private final CheckpointCoordinator coordinator;
    private final Object lock;
    private final CompletedCheckpointStore completedCheckpointStore;
    private final Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint;
    private final CheckpointStatsTracker statsTracker;
    private final double regionalMaxFailureRatio;
    private final int regionalMaxConsecutiveFailures;
    private final boolean regionalCheckpointEnabled;

    // Mutable state — all access must be under coordinator.lock
    private int consecutiveRegionalCheckpointCount = 0;
    private boolean forceGlobalNextCheckpoint = false;
    private Supplier<Boolean> allSourcesFinishedChecker = () -> false;
    @Nullable private Function<ExecutionVertexID, Object> regionIdProvider;

    RegionalCheckpointHandler(
            CheckpointCoordinator coordinator,
            Object lock,
            CompletedCheckpointStore completedCheckpointStore,
            Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
            CheckpointStatsTracker statsTracker,
            boolean regionalCheckpointEnabled,
            double regionalMaxFailureRatio,
            int regionalMaxConsecutiveFailures) {
        this.coordinator = coordinator;
        this.lock = lock;
        this.completedCheckpointStore = completedCheckpointStore;
        this.coordinatorsToCheckpoint = coordinatorsToCheckpoint;
        this.statsTracker = statsTracker;
        this.regionalCheckpointEnabled = regionalCheckpointEnabled;
        this.regionalMaxFailureRatio = regionalMaxFailureRatio;
        this.regionalMaxConsecutiveFailures = regionalMaxConsecutiveFailures;
    }

    // --------------------------------------------------------------------------------------------
    //  Public API (delegated from CheckpointCoordinator)
    // --------------------------------------------------------------------------------------------

    boolean isRegionalCheckpointEnabled() {
        return regionalCheckpointEnabled;
    }

    void setRegionIdProvider(Function<ExecutionVertexID, Object> regionIdProvider) {
        this.regionIdProvider = regionIdProvider;
    }

    void setAllSourcesFinishedChecker(Supplier<Boolean> allSourcesFinishedChecker) {
        this.allSourcesFinishedChecker = allSourcesFinishedChecker;
    }

    int getConsecutiveRegionalCheckpointCount() {
        return consecutiveRegionalCheckpointCount;
    }

    boolean getForceGlobalNextCheckpoint() {
        return forceGlobalNextCheckpoint;
    }

    /**
     * Checks if all sources are finished and, if so, forces the next checkpoint to be global. Per
     * FLIP-600 Section 9 "Bounded Source (Finished Operators)".
     */
    void checkAllSourcesFinishedAndForceGlobal(CheckpointProperties props) {
        if (regionalCheckpointEnabled && !props.isSavepoint() && allSourcesFinishedChecker.get()) {
            LOG.info("All sources finished; forcing next checkpoint to be global.");
            forceGlobalNextCheckpoint = true;
        }
    }

    /** Resets consecutive counter and force-global flag on successful global checkpoint. */
    void resetOnGlobalSuccess() {
        consecutiveRegionalCheckpointCount = 0;
        forceGlobalNextCheckpoint = false;
    }

    // --------------------------------------------------------------------------------------------
    //  Core regional checkpoint logic
    // --------------------------------------------------------------------------------------------

    /**
     * Evaluates a regional checkpoint after all tasks have responded with a mix of acknowledgements
     * and declines. Determines which pipeline regions have failed tasks, checks limits, assembles
     * state from the last completed checkpoint for failed regions, and completes the checkpoint
     * with only healthy regions being notified.
     *
     * <p>Important: This method should only be called in the checkpoint lock scope.
     */
    void tryCompleteRegionalCheckpoint(PendingCheckpoint checkpoint) {
        assert (Thread.holdsLock(lock));
        final long checkpointId = checkpoint.getCheckpointID();
        LOG.info("Regional Checkpoint evaluation triggered for checkpoint {}", checkpointId);

        // 1. Build mapping from ExecutionAttemptID to ExecutionVertex for declined tasks
        final Map<ExecutionAttemptID, CheckpointException> declinedTasks =
                checkpoint.getDeclinedTasks();
        final List<Execution> tasksToWaitFor = checkpoint.getCheckpointPlan().getTasksToWaitFor();
        final Map<ExecutionAttemptID, ExecutionVertex> attemptToVertex = new HashMap<>();
        for (Execution execution : tasksToWaitFor) {
            attemptToVertex.put(execution.getAttemptId(), execution.getVertex());
        }

        // 2. Check regionIdProvider availability
        if (regionIdProvider == null) {
            LOG.warn("Aborting regional checkpoint {} - regionIdProvider not set", checkpointId);
            coordinator.abortPendingCheckpoint(
                    checkpoint,
                    new CheckpointException(
                            "RegionIdProvider not set - cannot compute pipeline regions",
                            CheckpointFailureReason.CHECKPOINT_DECLINED));
            return;
        }

        // 3. Compute region membership for all vertices using subtask-level region objects
        final Map<Object, Set<ExecutionVertex>> regionToVertices = new IdentityHashMap<>();
        for (Execution execution : tasksToWaitFor) {
            ExecutionVertex ev = execution.getVertex();
            Object region = regionIdProvider.apply(ev.getID());
            regionToVertices.computeIfAbsent(region, k -> new HashSet<>()).add(ev);
        }
        final int totalRegions = regionToVertices.size();

        // 4. If single region → abort (ALL_TO_ALL topology, regional checkpoint not applicable)
        if (totalRegions <= 1) {
            LOG.info(
                    "Aborting regional checkpoint {} - job has only {} pipeline region(s)",
                    checkpointId,
                    totalRegions);
            coordinator.abortPendingCheckpoint(
                    checkpoint,
                    new CheckpointException(
                            "Single pipeline region - regional checkpoint not applicable",
                            CheckpointFailureReason.CHECKPOINT_DECLINED));
            return;
        }

        // 5. Determine failed regions (regions containing at least one declined task)
        final Set<Object> failedRegions = Collections.newSetFromMap(new IdentityHashMap<>());
        for (ExecutionAttemptID declinedAttemptId : declinedTasks.keySet()) {
            ExecutionVertex vertex = attemptToVertex.get(declinedAttemptId);
            if (vertex != null) {
                failedRegions.add(regionIdProvider.apply(vertex.getID()));
            }
        }

        // 6. Check failure ratio
        final double failureRatio = (double) failedRegions.size() / totalRegions;
        if (failureRatio > regionalMaxFailureRatio) {
            LOG.info(
                    "Aborting regional checkpoint {} - failure ratio {}/{} = {} exceeds max {}",
                    checkpointId,
                    failedRegions.size(),
                    totalRegions,
                    failureRatio,
                    regionalMaxFailureRatio);
            coordinator.abortPendingCheckpoint(
                    checkpoint,
                    new CheckpointException(
                            "Regional checkpoint failure ratio exceeded: "
                                    + failedRegions.size()
                                    + "/"
                                    + totalRegions,
                            CheckpointFailureReason.CHECKPOINT_DECLINED));
            return;
        }

        // 7. Tier 2: if the next checkpoint was forced to be global but still has declined
        //    tasks, abort and reset (per FLIP-600 two-tier max-consecutive-failures).
        if (forceGlobalNextCheckpoint) {
            LOG.info(
                    "Aborting checkpoint {} - forced global checkpoint still has declined tasks "
                            + "(Tier 2). Resetting consecutive count and force flag.",
                    checkpointId);
            forceGlobalNextCheckpoint = false;
            consecutiveRegionalCheckpointCount = 0;
            coordinator.abortPendingCheckpoint(
                    checkpoint,
                    new CheckpointException(
                            "Forced global checkpoint failed (Tier 2): "
                                    + declinedTasks.size()
                                    + " tasks declined",
                            CheckpointFailureReason.CHECKPOINT_DECLINED));
            return;
        }
        // FLIP-600 two-tier: current regional checkpoint completes; next is forced global.
        // 8. Get last completed checkpoint for fallback state
        final CompletedCheckpoint lastCompleted = completedCheckpointStore.getLatestCheckpoint();
        if (lastCompleted == null) {
            LOG.info(
                    "Aborting regional checkpoint {} - no historical checkpoint available",
                    checkpointId);
            coordinator.abortPendingCheckpoint(
                    checkpoint,
                    new CheckpointException(
                            "No historical checkpoint for regional fallback",
                            CheckpointFailureReason.CHECKPOINT_DECLINED));
            return;
        }
        final long fallbackCheckpointId = lastCompleted.getCheckpointID();

        // 9. Collect all vertices in failed regions
        final Set<ExecutionVertex> failedVertices = new HashSet<>();
        for (Object failedRegion : failedRegions) {
            Set<ExecutionVertex> regionVertices = regionToVertices.get(failedRegion);
            if (regionVertices != null) {
                failedVertices.addAll(regionVertices);
            }
        }

        // 10. Collect operator IDs in failed regions and check coordinator support
        final Set<OperatorID> failedRegionOperatorIds = new HashSet<>();
        for (ExecutionVertex ev : failedVertices) {
            ev.getJobVertex()
                    .getOperatorIDs()
                    .forEach(pair -> failedRegionOperatorIds.add(pair.getGeneratedOperatorID()));
        }

        for (OperatorCoordinatorCheckpointContext coordCtx : coordinatorsToCheckpoint) {
            if (failedRegionOperatorIds.contains(coordCtx.operatorId())) {
                if (!coordCtx.supportsRegionCheckpoint()) {
                    LOG.info(
                            "Aborting regional checkpoint {} - coordinator {} does not support "
                                    + "regional checkpoint",
                            checkpointId,
                            coordCtx.operatorId());
                    coordinator.abortPendingCheckpoint(
                            checkpoint,
                            new CheckpointException(
                                    "Coordinator "
                                            + coordCtx.operatorId()
                                            + " does not support regional checkpoint",
                                    CheckpointFailureReason.CHECKPOINT_DECLINED));
                    return;
                }
            }
        }

        // 11. Assemble state: for failed region subtasks use state from lastCompleted
        final Map<OperatorID, OperatorState> currentStates = checkpoint.getOperatorStates();
        final Map<OperatorID, OperatorState> lastCompletedStates =
                lastCompleted.getOperatorStates();

        // Build set of failed subtask indices per operator
        final Map<OperatorID, Set<Integer>> failedSubtasksByOperator = new HashMap<>();
        for (ExecutionVertex failedVertex : failedVertices) {
            final int subtaskIndex = failedVertex.getParallelSubtaskIndex();
            failedVertex
                    .getJobVertex()
                    .getOperatorIDs()
                    .forEach(
                            pair ->
                                    failedSubtasksByOperator
                                            .computeIfAbsent(
                                                    pair.getGeneratedOperatorID(),
                                                    k -> new HashSet<>())
                                            .add(subtaskIndex));
        }

        // Merge states: healthy subtask state from current checkpoint,
        // failed subtask state from last completed checkpoint
        for (Map.Entry<OperatorID, Set<Integer>> entry : failedSubtasksByOperator.entrySet()) {
            final OperatorID opId = entry.getKey();
            final Set<Integer> failedSubtasks = entry.getValue();
            final OperatorState lastState = lastCompletedStates.get(opId);
            final OperatorState currentState = currentStates.get(opId);

            if (lastState != null && currentState != null) {
                for (int subtaskIdx : failedSubtasks) {
                    OperatorSubtaskState fallbackState = lastState.getState(subtaskIdx);
                    if (fallbackState != null) {
                        currentState.putState(
                                subtaskIdx,
                                referenceFallbackState(
                                        fallbackState, fallbackCheckpointId, checkpointId));
                    }
                }
            } else if (lastState != null && currentState == null) {
                OperatorState newState =
                        new OperatorState(
                                null,
                                null,
                                opId,
                                lastState.getParallelism(),
                                lastState.getMaxParallelism());
                for (int subtaskIdx : failedSubtasks) {
                    OperatorSubtaskState fallbackState = lastState.getState(subtaskIdx);
                    if (fallbackState != null) {
                        newState.putState(
                                subtaskIdx,
                                referenceFallbackState(
                                        fallbackState, fallbackCheckpointId, checkpointId));
                    }
                }
                currentStates.put(opId, newState);
            }
        }

        // 12. Call checkpointCoordinatorForRegionFallback on coordinators in failed regions
        final List<CompletableFuture<Void>> coordinatorFutures = new ArrayList<>();
        for (OperatorCoordinatorCheckpointContext coordCtx : coordinatorsToCheckpoint) {
            if (failedRegionOperatorIds.contains(coordCtx.operatorId())) {
                Set<Integer> subtasksForCoordinator =
                        failedSubtasksByOperator.getOrDefault(
                                coordCtx.operatorId(), Collections.emptySet());
                if (!subtasksForCoordinator.isEmpty()) {
                    CompletableFuture<byte[]> resultFuture = new CompletableFuture<>();
                    try {
                        coordCtx.checkpointCoordinatorForRegionFallback(
                                checkpointId,
                                fallbackCheckpointId,
                                subtasksForCoordinator,
                                resultFuture);
                    } catch (Exception e) {
                        LOG.warn(
                                "Failed to invoke checkpointCoordinatorForRegionFallback on {}",
                                coordCtx.operatorId(),
                                e);
                        coordinator.abortPendingCheckpoint(
                                checkpoint,
                                new CheckpointException(
                                        "Region fallback coordinator call failed",
                                        CheckpointFailureReason.CHECKPOINT_DECLINED,
                                        e));
                        return;
                    }

                    final OperatorID opId = coordCtx.operatorId();
                    final int coordParallelism = coordCtx.currentParallelism();
                    final int coordMaxParallelism = coordCtx.maxParallelism();
                    coordinatorFutures.add(
                            resultFuture.thenAccept(
                                    bytes -> {
                                        synchronized (lock) {
                                            final ByteStreamStateHandle coordinatorStateHandle =
                                                    new ByteStreamStateHandle(
                                                            "regionFallback-" + opId, bytes);
                                            OperatorState state = currentStates.get(opId);
                                            if (state == null) {
                                                state =
                                                        new OperatorState(
                                                                null,
                                                                null,
                                                                opId,
                                                                coordParallelism,
                                                                coordMaxParallelism);
                                                currentStates.put(opId, state);
                                            }
                                            state.overwriteCoordinatorState(coordinatorStateHandle);
                                        }
                                    }));
                }
            }
        }

        // 13. Complete the checkpoint
        if (coordinatorFutures.isEmpty()) {
            completeRegionalCheckpoint(checkpoint, failedVertices, fallbackCheckpointId);
        } else {
            final PendingCheckpoint capturedCheckpoint = checkpoint;
            FutureUtils.combineAll(coordinatorFutures)
                    .thenAccept(
                            ignored -> {
                                synchronized (lock) {
                                    if (!capturedCheckpoint.isDisposed()) {
                                        completeRegionalCheckpoint(
                                                capturedCheckpoint,
                                                failedVertices,
                                                fallbackCheckpointId);
                                    }
                                }
                            })
                    .exceptionally(
                            t -> {
                                synchronized (lock) {
                                    if (!capturedCheckpoint.isDisposed()) {
                                        coordinator.abortPendingCheckpoint(
                                                capturedCheckpoint,
                                                new CheckpointException(
                                                        "Region fallback future failed",
                                                        CheckpointFailureReason.CHECKPOINT_DECLINED,
                                                        t));
                                    }
                                }
                                return null;
                            });
        }
    }

    /**
     * Marks a fallback subtask state as referencing the historical checkpoint it originates from,
     * and registers its shared state under the new checkpoint id.
     */
    private OperatorSubtaskState referenceFallbackState(
            OperatorSubtaskState fallbackState, long fallbackCheckpointId, long checkpointId) {
        assert (Thread.holdsLock(lock));
        final OperatorSubtaskState referencedState =
                fallbackState.toBuilder().setRefCheckpointId(fallbackCheckpointId).build();
        referencedState.registerSharedStates(
                completedCheckpointStore.getSharedStateRegistry(), checkpointId);
        return referencedState;
    }

    /** Completes a regional checkpoint by finalizing it and notifying only healthy region tasks. */
    private void completeRegionalCheckpoint(
            PendingCheckpoint checkpoint,
            Set<ExecutionVertex> failedVertices,
            long fallbackCheckpointId) {
        assert (Thread.holdsLock(lock));
        try {
            final long checkpointId = checkpoint.getCheckpointID();

            // Report stats for subtasks in failed regions
            final long fallbackStatsTimestamp = System.currentTimeMillis();
            for (ExecutionVertex failedVertex : failedVertices) {
                checkpoint.reportFallbackSubtaskStats(
                        failedVertex.getJobvertexId(),
                        failedVertex.getParallelSubtaskIndex(),
                        fallbackStatsTimestamp,
                        fallbackCheckpointId);
            }

            completedCheckpointStore.getSharedStateRegistry().checkpointCompleted(checkpointId);

            final CompletedCheckpoint completedCheckpoint =
                    checkpoint.finalizeRegionalCheckpoint(
                            coordinator.getCheckpointsCleaner(),
                            coordinator::scheduleTriggerRequest,
                            coordinator.getExecutor());
            Preconditions.checkState(checkpoint.isDisposed() && completedCheckpoint != null);

            final CompletedCheckpoint lastSubsumed =
                    coordinator.addCompletedCheckpointToStoreAndSubsumeOldest(
                            checkpointId, completedCheckpoint, checkpoint);

            coordinator.reportCompletedCheckpoint(completedCheckpoint);
            checkpoint.getCompletionFuture().complete(completedCheckpoint);

            coordinator.removePendingCheckpoint(checkpointId);
            coordinator.scheduleTriggerRequest();

            // Increment consecutive regional checkpoint counter
            consecutiveRegionalCheckpointCount++;
            statsTracker.reportRegionalCheckpointCompleted();

            // Tier 1: if consecutive count reaches the limit, force the NEXT checkpoint
            // to be global.
            if (consecutiveRegionalCheckpointCount >= regionalMaxConsecutiveFailures
                    && !forceGlobalNextCheckpoint) {
                LOG.info(
                        "Regional checkpoint {} completed. Consecutive count {} reached max {}. "
                                + "Next checkpoint will be forced global (Tier 1).",
                        checkpointId,
                        consecutiveRegionalCheckpointCount,
                        regionalMaxConsecutiveFailures);
                forceGlobalNextCheckpoint = true;
            }

            coordinator.setLastCheckpointCompletionRelativeTime(
                    coordinator.getClock().relativeTimeMillis());
            coordinator.logCheckpointInfo(completedCheckpoint);

            // Drop subsumed checkpoints
            coordinator.dropSubsumedCheckpoints(checkpointId);

            // Notify only healthy region tasks
            final List<ExecutionVertex> healthyTasks =
                    checkpoint.getCheckpointPlan().getTasksToCommitTo().stream()
                            .filter(ev -> !failedVertices.contains(ev))
                            .collect(Collectors.toList());

            // Build RegionalCheckpointInfo for coordinators
            final Set<String> fallbackSubtaskIdentifiers = new HashSet<>();
            for (ExecutionVertex ev : failedVertices) {
                fallbackSubtaskIdentifiers.add(ev.getTaskNameWithSubtaskIndex());
            }
            final Map<Long, Set<String>> fallbackMap = new HashMap<>();
            fallbackMap.put(fallbackCheckpointId, fallbackSubtaskIdentifiers);
            final RegionalCheckpointInfo regionalInfo = new RegionalCheckpointInfo(fallbackMap);

            // Send ack to healthy region tasks only
            final long lastSubsumedId = coordinator.extractIdIfDiscardedOnSubsumed(lastSubsumed);
            sendAcknowledgeMessagesToTasks(
                    healthyTasks, checkpointId, completedCheckpoint.getTimestamp(), lastSubsumedId);

            // Notify failed-region tasks with fallbackCheckpointId
            for (ExecutionVertex ev :
                    checkpoint.getCheckpointPlan().getTasksToCommitTo().stream()
                            .filter(failedVertices::contains)
                            .collect(Collectors.toList())) {
                Execution ee = ev.getCurrentExecutionAttempt();
                if (ee != null) {
                    ee.notifyCheckpointOnComplete(
                            checkpointId,
                            completedCheckpoint.getTimestamp(),
                            lastSubsumedId,
                            fallbackCheckpointId);
                }
            }

            // Notify coordinators: healthy → notifyRegionalCheckpointComplete, failed →
            // notifyRegionalCheckpointFallback
            final Set<OperatorID> failedRegionOps =
                    failedVertices.stream()
                            .flatMap(
                                    ev ->
                                            ev.getJobVertex().getOperatorIDs().stream()
                                                    .map(OperatorIDPair::getGeneratedOperatorID))
                            .collect(Collectors.toSet());
            for (OperatorCoordinatorCheckpointContext coordinatorContext :
                    coordinatorsToCheckpoint) {
                if (failedRegionOps.contains(coordinatorContext.operatorId())) {
                    coordinatorContext.notifyRegionalCheckpointFallback(
                            checkpointId, fallbackCheckpointId);
                } else {
                    coordinatorContext.notifyRegionalCheckpointComplete(checkpointId, regionalInfo);
                }
            }

            LOG.info(
                    "Regional checkpoint {} completed successfully. "
                            + "Notified {} healthy tasks, {} tasks in failed regions.",
                    checkpointId,
                    healthyTasks.size(),
                    failedVertices.size());

        } catch (Exception e) {
            checkpoint.getCompletionFuture().completeExceptionally(e);
            if (!checkpoint.isDisposed()) {
                coordinator.abortPendingCheckpoint(
                        checkpoint,
                        new CheckpointException(
                                CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e));
            }
        }
    }

    /**
     * Sends checkpoint-complete notifications to the given tasks (without coordinator
     * notification).
     */
    private void sendAcknowledgeMessagesToTasks(
            List<ExecutionVertex> tasksToCommit,
            long completedCheckpointId,
            long completedTimestamp,
            long lastSubsumedCheckpointId) {
        for (ExecutionVertex ev : tasksToCommit) {
            Execution ee = ev.getCurrentExecutionAttempt();
            if (ee != null) {
                ee.notifyCheckpointOnComplete(
                        completedCheckpointId, completedTimestamp, lastSubsumedCheckpointId);
            }
        }
    }
}

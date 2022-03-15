/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The checkpoint failure manager which centralized manage checkpoint failure processing logic. */
public class CheckpointFailureManager {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointFailureManager.class);

    public static final int UNLIMITED_TOLERABLE_FAILURE_NUMBER = Integer.MAX_VALUE;
    public static final String EXCEEDED_CHECKPOINT_TOLERABLE_FAILURE_MESSAGE =
            "Exceeded checkpoint tolerable failure threshold.";
    private static final int UNKNOWN_CHECKPOINT_ID = -1;

    private final int tolerableCpFailureNumber;
    private final FailJobCallback failureCallback;
    private final AtomicInteger continuousFailureCounter;
    private final Set<Long> countedCheckpointIds;
    private long lastSucceededCheckpointId = Long.MIN_VALUE;

    public CheckpointFailureManager(int tolerableCpFailureNumber, FailJobCallback failureCallback) {
        checkArgument(
                tolerableCpFailureNumber >= 0,
                "The tolerable checkpoint failure number is illegal, "
                        + "it must be greater than or equal to 0 .");
        this.tolerableCpFailureNumber = tolerableCpFailureNumber;
        this.continuousFailureCounter = new AtomicInteger(0);
        this.failureCallback = checkNotNull(failureCallback);
        this.countedCheckpointIds = ConcurrentHashMap.newKeySet();
    }

    /**
     * Failures on JM:
     *
     * <ul>
     *   <li>all checkpoints - go against failure counter.
     *   <li>any savepoints - don’t do anything, manual action, the failover will not help anyway.
     * </ul>
     *
     * <p>Failures on TM:
     *
     * <ul>
     *   <li>all checkpoints - go against failure counter (failover might help and we want to notify
     *       users).
     *   <li>sync savepoints - we must always fail, otherwise we risk deadlock when the job
     *       cancelation waiting for finishing savepoint which never happens.
     *   <li>non sync savepoints - go against failure counter (failover might help solve the
     *       problem).
     * </ul>
     *
     * @param pendingCheckpoint the failed checkpoint if it was initialized already.
     * @param checkpointProperties the checkpoint properties in order to determinate which handle
     *     strategy can be used.
     * @param exception the checkpoint exception.
     * @param executionAttemptID the execution attempt id, as a safe guard.
     * @param job the JobID.
     * @param pendingCheckpointStats the pending checkpoint statistics.
     * @param statsTracker the tracker for checkpoint statistics.
     */
    public void handleCheckpointException(
            @Nullable PendingCheckpoint pendingCheckpoint,
            CheckpointProperties checkpointProperties,
            CheckpointException exception,
            @Nullable ExecutionAttemptID executionAttemptID,
            JobID job,
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            CheckpointStatsTracker statsTracker) {
        long checkpointId =
                pendingCheckpoint == null
                        ? UNKNOWN_CHECKPOINT_ID
                        : pendingCheckpoint.getCheckpointID();
        updateStatsAfterCheckpointFailed(pendingCheckpointStats, statsTracker, exception);

        if (CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING.equals(
                exception.getCheckpointFailureReason())) {
            LOG.info(
                    "Failed to trigger checkpoint for job {} since {}.",
                    job,
                    exception.getMessage());
        } else {
            LOG.warn(
                    "Failed to trigger or complete checkpoint {} for job {}. ({} consecutive failed attempts so far)",
                    checkpointId == UNKNOWN_CHECKPOINT_ID ? "UNKNOWN_CHECKPOINT_ID" : checkpointId,
                    job,
                    continuousFailureCounter.get(),
                    exception);
        }
        if (isJobManagerFailure(exception, executionAttemptID)) {
            handleJobLevelCheckpointException(checkpointProperties, exception, checkpointId);
        } else {
            handleTaskLevelCheckpointException(
                    checkNotNull(pendingCheckpoint), exception, checkNotNull(executionAttemptID));
        }
    }

    /**
     * Updating checkpoint statistics after checkpoint failed.
     *
     * @param pendingCheckpointStats the pending checkpoint statistics.
     * @param exception the checkpoint exception.
     */
    private void updateStatsAfterCheckpointFailed(
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            CheckpointStatsTracker statsTracker,
            CheckpointException exception) {
        if (pendingCheckpointStats != null) {
            long failureTimestamp = System.currentTimeMillis();
            pendingCheckpointStats.reportFailedCheckpoint(failureTimestamp, exception);
        } else {
            statsTracker.reportFailedCheckpointsWithoutInProgress();
        }
    }

    private boolean isJobManagerFailure(
            CheckpointException exception, @Nullable ExecutionAttemptID executionAttemptID) {
        // TODO: Try to get rid of checking nullability of executionAttemptID because false value of
        // isPreFlightFailure should guarantee that executionAttemptID is always not null.
        return isPreFlightFailure(exception) || executionAttemptID == null;
    }

    /**
     * Handle job level checkpoint exception with a handler callback.
     *
     * @param exception the checkpoint exception.
     * @param checkpointId the failed checkpoint id used to count the continuous failure number
     *     based on checkpoint id sequence. In trigger phase, we may not get the checkpoint id when
     *     the failure happens before the checkpoint id generation. In this case, it will be
     *     specified a negative latest generated checkpoint id as a special flag.
     */
    void handleJobLevelCheckpointException(
            CheckpointProperties checkpointProperties,
            CheckpointException exception,
            long checkpointId) {
        if (!checkpointProperties.isSavepoint()) {
            checkFailureAgainstCounter(exception, checkpointId, failureCallback::failJob);
        }
    }

    /**
     * Handle task level checkpoint exception with a handler callback.
     *
     * @param pendingCheckpoint the failed checkpoint used to count the continuous failure number
     *     based on checkpoint id sequence. In trigger phase, we may not get the checkpoint id when
     *     the failure happens before the checkpoint id generation. In this case, it will be
     *     specified a negative latest generated checkpoint id as a special flag.
     * @param exception the checkpoint exception.
     * @param executionAttemptID the execution attempt id, as a safe guard.
     */
    void handleTaskLevelCheckpointException(
            PendingCheckpoint pendingCheckpoint,
            CheckpointException exception,
            ExecutionAttemptID executionAttemptID) {
        CheckpointProperties checkpointProps = pendingCheckpoint.getProps();
        if (checkpointProps.isSavepoint() && checkpointProps.isSynchronous()) {
            failureCallback.failJob(exception);
        } else {
            checkFailureAgainstCounter(
                    exception,
                    pendingCheckpoint.getCheckpointID(),
                    e -> failureCallback.failJobDueToTaskFailure(e, executionAttemptID));
        }
    }

    private void checkFailureAgainstCounter(
            CheckpointException exception,
            long checkpointId,
            Consumer<FlinkRuntimeException> errorHandler) {
        if (checkpointId == UNKNOWN_CHECKPOINT_ID || checkpointId > lastSucceededCheckpointId) {
            checkFailureCounter(exception, checkpointId);
            if (continuousFailureCounter.get() > tolerableCpFailureNumber) {
                clearCount();
                errorHandler.accept(
                        new FlinkRuntimeException(EXCEEDED_CHECKPOINT_TOLERABLE_FAILURE_MESSAGE));
            }
        }
    }

    public void checkFailureCounter(CheckpointException exception, long checkpointId) {
        if (tolerableCpFailureNumber == UNLIMITED_TOLERABLE_FAILURE_NUMBER) {
            return;
        }

        CheckpointFailureReason reason = exception.getCheckpointFailureReason();
        switch (reason) {
            case PERIODIC_SCHEDULER_SHUTDOWN:
            case TOO_MANY_CONCURRENT_CHECKPOINTS:
            case TOO_MANY_CHECKPOINT_REQUESTS:
            case MINIMUM_TIME_BETWEEN_CHECKPOINTS:
            case NOT_ALL_REQUIRED_TASKS_RUNNING:
            case CHECKPOINT_SUBSUMED:
            case CHECKPOINT_COORDINATOR_SUSPEND:
            case CHECKPOINT_COORDINATOR_SHUTDOWN:
            case JOB_FAILURE:
            case JOB_FAILOVER_REGION:
                // for compatibility purposes with user job behavior
            case CHECKPOINT_DECLINED_TASK_NOT_READY:
            case CHECKPOINT_DECLINED_TASK_CLOSING:
            case CHECKPOINT_DECLINED_TASK_NOT_CHECKPOINTING:
            case CHECKPOINT_DECLINED_ALIGNMENT_LIMIT_EXCEEDED:
            case CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER:
            case CHECKPOINT_DECLINED_SUBSUMED:
            case CHECKPOINT_DECLINED_INPUT_END_OF_STREAM:

            case TASK_FAILURE:
            case TASK_CHECKPOINT_FAILURE:
            case UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE:
            case TRIGGER_CHECKPOINT_FAILURE:
            case FINALIZE_CHECKPOINT_FAILURE:
                // ignore
                break;

            case IO_EXCEPTION:
            case CHECKPOINT_ASYNC_EXCEPTION:
            case CHECKPOINT_DECLINED:
            case CHECKPOINT_EXPIRED:
                // we should make sure one checkpoint only be counted once
                if (checkpointId == UNKNOWN_CHECKPOINT_ID
                        || countedCheckpointIds.add(checkpointId)) {
                    continuousFailureCounter.incrementAndGet();
                }

                break;

            default:
                throw new FlinkRuntimeException(
                        "Unknown checkpoint failure reason : " + reason.name());
        }
    }

    /**
     * Handle checkpoint success.
     *
     * @param checkpointId the failed checkpoint id used to count the continuous failure number
     *     based on checkpoint id sequence.
     */
    public void handleCheckpointSuccess(long checkpointId) {
        if (checkpointId > lastSucceededCheckpointId) {
            lastSucceededCheckpointId = checkpointId;
            clearCount();
        }
    }

    private void clearCount() {
        continuousFailureCounter.set(0);
        countedCheckpointIds.clear();
    }

    private static boolean isPreFlightFailure(final Throwable cause) {
        return ExceptionUtils.findThrowable(cause, CheckpointException.class)
                .map(CheckpointException::getCheckpointFailureReason)
                .map(CheckpointFailureReason::isPreFlight)
                .orElse(false);
    }

    /** A callback interface about how to fail a job. */
    public interface FailJobCallback {

        /**
         * Fails the whole job graph.
         *
         * @param cause The reason why the synchronous savepoint fails.
         */
        void failJob(final Throwable cause);

        /**
         * Fails the whole job graph due to task failure.
         *
         * @param cause The reason why the job is cancelled.
         * @param failingTask The id of the failing task attempt to prevent failing the job multiple
         *     times.
         */
        void failJobDueToTaskFailure(final Throwable cause, final ExecutionAttemptID failingTask);
    }
}

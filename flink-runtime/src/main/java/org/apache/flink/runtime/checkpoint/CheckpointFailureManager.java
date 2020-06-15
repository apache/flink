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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_EXPIRED;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint failure manager which centralized manage checkpoint failure processing logic.
 */
public class CheckpointFailureManager {

	public static final int UNLIMITED_TOLERABLE_FAILURE_NUMBER = Integer.MAX_VALUE;
	private static final Set<CheckpointFailureReason> FAILURE_REASONS = EnumSet.of(CHECKPOINT_DECLINED, CHECKPOINT_EXPIRED);

	private final int tolerableCpFailureNumber;
	private final FailJobCallback failureCallback;
	private final AtomicInteger continuousFailureCounter;
	private final Set<Long> countedCheckpointIds;

	public CheckpointFailureManager(int tolerableCpFailureNumber, FailJobCallback failureCallback) {
		checkArgument(tolerableCpFailureNumber >= 0,
			"The tolerable checkpoint failure number is illegal, " +
				"it must be greater than or equal to 0 .");
		this.tolerableCpFailureNumber = tolerableCpFailureNumber;
		this.continuousFailureCounter = new AtomicInteger(0);
		this.failureCallback = checkNotNull(failureCallback);
		this.countedCheckpointIds = ConcurrentHashMap.newKeySet();
	}

	/**
	 * Handle job level checkpoint exception with a handler callback.
	 */
	public void handleJobLevelCheckpointException(CheckpointException exception, Optional<Long> checkpointId) {
		handleException(exception, checkpointId, FailJobCallback::failJob);
	}

	/**
	 * Handle task level checkpoint exception with a handler callback.
	 *
	 * @param exception the checkpoint exception.
	 * @param checkpointId the failed checkpoint id used to count the continuous failure number based on
	 *                     checkpoint id sequence. In trigger phase, we may not get the checkpoint id when the failure
	 *                     happens before the checkpoint id generation. In this case, it will be specified a negative
	 *                      latest generated checkpoint id as a special flag.
	 * @param executionAttemptID the execution attempt id, as a safe guard.
	 */
	public void handleTaskLevelCheckpointException(
			CheckpointException exception,
			long checkpointId,
			ExecutionAttemptID executionAttemptID) {
		handleException(exception, Optional.of(checkpointId), (failureCallback, e) -> failureCallback.failJobDueToTaskFailure(e, executionAttemptID));
	}

	private void handleException(CheckpointException exception, Optional<Long> checkpointId, BiConsumer<FailJobCallback, Exception> onFailure) {
		if (isCheckpointFailure(exception) &&
				!exceededTolerableFailures() && // prevent unnecessary storing checkpointId
				checkAndAdd(checkpointId) &&
				continuousFailureCounter.incrementAndGet() == tolerableCpFailureNumber + 1) {
			countedCheckpointIds.clear();
			onFailure.accept(failureCallback, new FlinkRuntimeException("Exceeded checkpoint tolerable failure threshold."));
		}
	}

	private boolean isCheckpointFailure(CheckpointException exception) {
		return tolerableCpFailureNumber != UNLIMITED_TOLERABLE_FAILURE_NUMBER && FAILURE_REASONS.contains(exception.getCheckpointFailureReason());
	}

	private Boolean checkAndAdd(Optional<Long> checkpointId) {
		return checkpointId.map(countedCheckpointIds::add).orElse(true);
	}

	private boolean exceededTolerableFailures() {
		return continuousFailureCounter.get() > tolerableCpFailureNumber;
	}

	/**
	 * Handle checkpoint success.
	 *
	 * @param checkpointId the failed checkpoint id used to count the continuous failure number based on
	 *                     checkpoint id sequence.
	 */
	public void handleCheckpointSuccess(long checkpointId) {
		continuousFailureCounter.set(0);
		countedCheckpointIds.remove(checkpointId);
	}

	/**
	 * Fails the whole job graph in case an in-progress synchronous savepoint is discarded.
	 *
	 * <p>If the checkpoint was cancelled at the checkpoint coordinator, i.e. before
	 * the synchronous savepoint barrier was sent to the tasks, then we do not cancel the job
	 * as we do not risk having a deadlock.
	 *
	 * @param cause The reason why the job is cancelled.
	 * */
	void handleSynchronousSavepointFailure(final Throwable cause) {
		if (!isPreFlightFailure(cause)) {
			failureCallback.failJob(cause);
		}
	}

	private static boolean isPreFlightFailure(final Throwable cause) {
		return ExceptionUtils.findThrowable(cause, CheckpointException.class)
				.map(CheckpointException::getCheckpointFailureReason)
				.map(CheckpointFailureReason::isPreFlight)
				.orElse(false);
	}

	/**
	 * A callback interface about how to fail a job.
	 */
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
		 * @param failingTask The id of the failing task attempt to prevent failing the job multiple times.
		 */
		void failJobDueToTaskFailure(final Throwable cause, final ExecutionAttemptID failingTask);

	}

}

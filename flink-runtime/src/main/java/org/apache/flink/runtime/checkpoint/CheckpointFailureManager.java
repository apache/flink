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

import org.apache.flink.util.FlinkRuntimeException;

import java.util.Iterator;
import java.util.TreeSet;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint failure manager which centralized manage checkpoint failure processing logic.
 */
public class CheckpointFailureManager {

	private final static int UNLIMITED_TOLERABLE_FAILURE_NUMBER = Integer.MAX_VALUE;

	private final int tolerableCpFailureNumber;
	private final FailJobCallback failureCallback;
	private final TreeSet<Long> failedCheckpointIds;
	private final Object lock = new Object();
	private long maxSuccessCheckpointId;

	public CheckpointFailureManager(int tolerableCpFailureNumber, FailJobCallback failureCallback) {
		checkArgument(tolerableCpFailureNumber >= 0,
			"The tolerable checkpoint failure number is illegal, " +
				"it must be greater than or equal to 0 .");
		this.tolerableCpFailureNumber = tolerableCpFailureNumber;
		this.failureCallback = checkNotNull(failureCallback);
		this.failedCheckpointIds = new TreeSet<>();
		this.maxSuccessCheckpointId = 0;
	}

	/**
	 * Handle checkpoint exception with a handler callback.
	 *
	 * @param exception the checkpoint exception.
	 * @param checkpointId the failed checkpoint id used to count the continuous failure number based on
	 *                     checkpoint id sequence. In trigger phase, we may not get the checkpoint id when the failure
	 *                     happens before the checkpoint id generation. In this case, it will be specified a negative
	 *                      latest generated checkpoint id as a special flag.
	 */
	public void handleCheckpointException(CheckpointException exception, long checkpointId) {
		if (tolerableCpFailureNumber == UNLIMITED_TOLERABLE_FAILURE_NUMBER) {
			return;
		}

		synchronized (lock) {
			CheckpointFailureReason reason = exception.getCheckpointFailureReason();
			switch (reason) {
				case PERIODIC_SCHEDULER_SHUTDOWN:
				case ALREADY_QUEUED:
				case TOO_MANY_CONCURRENT_CHECKPOINTS:
				case MINIMUM_TIME_BETWEEN_CHECKPOINTS:
				case NOT_ALL_REQUIRED_TASKS_RUNNING:
				case CHECKPOINT_SUBSUMED:
				case CHECKPOINT_COORDINATOR_SUSPEND:
				case CHECKPOINT_COORDINATOR_SHUTDOWN:
				case JOB_FAILURE:
				case JOB_FAILOVER_REGION:
					//for compatibility purposes with user job behavior
				case CHECKPOINT_DECLINED_TASK_NOT_READY:
				case CHECKPOINT_DECLINED_TASK_NOT_CHECKPOINTING:
				case CHECKPOINT_DECLINED_ALIGNMENT_LIMIT_EXCEEDED:
				case CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER:
				case CHECKPOINT_DECLINED_SUBSUMED:
				case CHECKPOINT_DECLINED_INPUT_END_OF_STREAM:

				case EXCEPTION:
				case CHECKPOINT_EXPIRED:
				case TASK_CHECKPOINT_FAILURE:
				case TRIGGER_CHECKPOINT_FAILURE:
				case FINALIZE_CHECKPOINT_FAILURE:
					//ignore
					break;

				case CHECKPOINT_DECLINED:
					//we should make sure one checkpoint only be counted once
					if (!failedCheckpointIds.contains(checkpointId)) {
						failedCheckpointIds.add(checkpointId);
					}

					break;

				default:
					throw new FlinkRuntimeException("Unknown checkpoint failure reason : " + reason.name());
			}

			if (failedCheckpointIds.contains(checkpointId) && (checkContinuityForward(checkpointId) || countContinuityBack(checkpointId))) {
				clearAllFailedCheckpointIds();
				failureCallback.failJob();
			}
		}
	}

	/**
	 * Handle checkpoint success.
	 *
	 * @param checkpointId the failed checkpoint id used to count the continuous failure number based on
	 *                     checkpoint id sequence.
	 */
	public void handleCheckpointSuccess(long checkpointId) {
		synchronized (lock) {
			//remember the maximum success checkpoint id
			maxSuccessCheckpointId = checkpointId > maxSuccessCheckpointId ? checkpointId : maxSuccessCheckpointId;

			if (failedCheckpointIds.size() == 0) {
				return;
			}

			Long latestCheckpointId = failedCheckpointIds.last();
			boolean needClearAll = latestCheckpointId == null ? true : checkpointId > latestCheckpointId;

			if (needClearAll) {
				clearAllFailedCheckpointIds();
			} else {
				clearFailedCheckpointIdsBefore(checkpointId);
			}
		}
	}

	private void clearAllFailedCheckpointIds() {
		failedCheckpointIds.clear();
	}

	private void clearFailedCheckpointIdsBefore(long currentCheckpointId) {
		Iterator<Long> descendingIterator = failedCheckpointIds.descendingIterator();
		while (descendingIterator.hasNext()) {
			if (descendingIterator.next() < currentCheckpointId) {
				descendingIterator.remove();
			}
		}
	}

	/**
	 * Check continuity from current checkpoint id forward the further checkpoint id.
	 */
	private boolean checkContinuityForward(long currentCheckpointId) {
		//short out
		if (failedCheckpointIds.size() == 0 || currentCheckpointId < maxSuccessCheckpointId) {
			return false;
		}

		Long oldLatestCheckpointId = failedCheckpointIds.last();
		if (oldLatestCheckpointId <= currentCheckpointId ||
			oldLatestCheckpointId - currentCheckpointId < tolerableCpFailureNumber - 1) {

			return false;
		}

		boolean flag = true;
		for (int i = 1; i <= tolerableCpFailureNumber; i++) {
			if (!failedCheckpointIds.contains(currentCheckpointId + i)) {
				flag = false;
				break;
			}
		}

		return flag;
	}

	/**
	 * Check continuity from current checkpoint id back to the old checkpoint ids.
	 */
	private boolean countContinuityBack(long currentCheckpointId) {
		if (failedCheckpointIds.size() == 0 || currentCheckpointId < maxSuccessCheckpointId) {
			return false;
		}

		Long firstCheckpointId = failedCheckpointIds.first();
		if (currentCheckpointId - firstCheckpointId < tolerableCpFailureNumber - 1) {
			return false;
		}

		boolean flag = true;
		for (int i = 1; i <= tolerableCpFailureNumber; i++) {
			if (!failedCheckpointIds.contains(currentCheckpointId - i)) {
				flag = false;
				break;
			}
		}

		return flag;
	}

	/**
	 * A callback interface about how to fail a job.
	 */
	public interface FailJobCallback {

		void failJob();

	}

}

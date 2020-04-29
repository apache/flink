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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator.CheckpointTriggerRequest;
import org.apache.flink.runtime.util.clock.Clock;

import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.TOO_MANY_CONCURRENT_CHECKPOINTS;

@SuppressWarnings("ConstantConditions")
class CheckpointRequestDecider {
	private static final int MAX_QUEUED_REQUESTS = 1000;

	private final int maxConcurrentCheckpointAttempts;
	private final Consumer<Long> rescheduleTrigger;
	private final Clock clock;
	private final long minPauseBetweenCheckpoints;
	private final Supplier<Integer> pendingCheckpointsSizeSupplier;
	private final Object lock;
	private final PriorityQueue<CheckpointTriggerRequest> triggerRequestQueue = new PriorityQueue<>(checkpointTriggerRequestsComparator());

	CheckpointRequestDecider(
			int maxConcurrentCheckpointAttempts,
			Consumer<Long> rescheduleTrigger,
			Clock clock,
			long minPauseBetweenCheckpoints,
			Supplier<Integer> pendingCheckpointsSizeSupplier,
			Object lock) {
		this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;
		this.rescheduleTrigger = rescheduleTrigger;
		this.clock = clock;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.pendingCheckpointsSizeSupplier = pendingCheckpointsSizeSupplier;
		this.lock = lock;
	}

	Optional<CheckpointTriggerRequest> chooseRequestToExecute(CheckpointTriggerRequest newRequest, boolean isTriggering, long lastCompletionMs) {
		return chooseRequestToExecute(false, () -> newRequest, isTriggering, lastCompletionMs);
	}

	Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute(boolean isTriggering, long lastCompletionMs) {
		return chooseRequestToExecute(true, triggerRequestQueue::peek, isTriggering, lastCompletionMs);
	}

	/**
	 * Choose the next {@link CheckpointTriggerRequest request} to execute based on the provided candidate and the
	 * current state. Acquires a lock and may update the state.
	 * @return request to execute, if any.
	 */
	private Optional<CheckpointTriggerRequest> chooseRequestToExecute(
			boolean isEnqueued,
			Supplier<CheckpointTriggerRequest> candidateSupplier,
			boolean isTriggering,
			long lastCompletionMs) {
		synchronized (lock) {
			final CheckpointTriggerRequest candidate = candidateSupplier.get();
			if (candidate == null) {
				return Optional.empty();
			} else if (isTriggering) {
				return onCantTriggerNow(candidate, isEnqueued);
			} else if (pendingCheckpointsSizeSupplier.get() >= maxConcurrentCheckpointAttempts) {
				return onCantCheckpointNow(candidate, TOO_MANY_CONCURRENT_CHECKPOINTS, () -> { /* not used */ }, isEnqueued);
			} else if (nextTriggerDelayMillis(lastCompletionMs) > 0) {
				return onCantCheckpointNow(candidate, MINIMUM_TIME_BETWEEN_CHECKPOINTS, () -> rescheduleTrigger.accept(nextTriggerDelayMillis(lastCompletionMs)), isEnqueued);
			} else if (candidate != triggerRequestQueue.peek()) {
				triggerRequestQueue.offer(candidate);
				return Optional.of(triggerRequestQueue.poll());
			} else {
				return Optional.of(isEnqueued ? triggerRequestQueue.poll() : candidate);
			}
		}
	}

	private Optional<CheckpointTriggerRequest> onCantTriggerNow(CheckpointTriggerRequest candidate, boolean isEnqueued) {
		offerIfNeeded(candidate, isEnqueued);
		return Optional.empty();
	}

	private Optional<CheckpointTriggerRequest> onCantCheckpointNow(
			CheckpointTriggerRequest candidate,
			CheckpointFailureReason dropReason,
			Runnable postDrop,
			boolean isEnqueued) {
		if (candidate.props.forceCheckpoint()) {
			return Optional.of(isEnqueued ? triggerRequestQueue.poll() : candidate);
		} else if (candidate.isPeriodic) {
			if (isEnqueued) {
				triggerRequestQueue.poll();
			}
			candidate.completeExceptionally(new CheckpointException(dropReason));
			postDrop.run();
		}
		offerIfNeeded(candidate, isEnqueued);
		return Optional.empty();
	}

	private void offerIfNeeded(CheckpointTriggerRequest candidate, boolean isEnqueued) {
		if (!isEnqueued) {
			if (triggerRequestQueue.size() < MAX_QUEUED_REQUESTS) {
				triggerRequestQueue.offer(candidate);
			} else {
				candidate.completeExceptionally(new CheckpointException(CheckpointFailureReason.TOO_MANY_CHECKPOINT_REQUESTS));
			}
		}
	}

	private long nextTriggerDelayMillis(long lastCheckpointCompletionRelativeTime) {
		return lastCheckpointCompletionRelativeTime - clock.relativeTimeMillis() + minPauseBetweenCheckpoints;
	}

	@VisibleForTesting
	PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
		synchronized (lock) {
			return triggerRequestQueue;
		}
	}

	void abortAll(CheckpointException exception) {
		CheckpointTriggerRequest request;
		while ((request = getTriggerRequestQueue().poll()) != null) {
			request.completeExceptionally(exception);
		}
	}

	private static Comparator<CheckpointTriggerRequest> checkpointTriggerRequestsComparator() {
		return (r1, r2) -> {
			if (r1.props.isSavepoint() != r2.props.isSavepoint()) {
				return r1.props.isSavepoint() ? -1 : 1;
			} else if (r1.props.forceCheckpoint() != r2.props.forceCheckpoint()) {
				return r1.props.forceCheckpoint() ? -1 : 1;
			} else if (r1.isPeriodic != r2.isPeriodic) {
				return r1.isPeriodic ? 1 : -1;
			} else {
				return Long.compare(r1.timestamp, r2.timestamp);
			}
		};
	}

}

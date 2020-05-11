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
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.identityHashCode;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.TOO_MANY_CHECKPOINT_REQUESTS;

@SuppressWarnings("ConstantConditions")
class CheckpointRequestDecider {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointRequestDecider.class);
	private static final int LOG_TIME_IN_QUEUE_THRESHOLD_MS = 100;
	private static final int DEFAULT_MAX_QUEUED_REQUESTS = 1000;

	private final int maxConcurrentCheckpointAttempts;
	private final Consumer<Long> rescheduleTrigger;
	private final Clock clock;
	private final long minPauseBetweenCheckpoints;
	private final Supplier<Integer> pendingCheckpointsSizeSupplier;
	private final Object lock;
	@GuardedBy("lock")
	private final NavigableSet<CheckpointTriggerRequest> queuedRequests = new TreeSet<>(checkpointTriggerRequestsComparator());
	private final int maxQueuedRequests;

	CheckpointRequestDecider(
			int maxConcurrentCheckpointAttempts,
			Consumer<Long> rescheduleTrigger,
			Clock clock,
			long minPauseBetweenCheckpoints,
			Supplier<Integer> pendingCheckpointsSizeSupplier,
			Object lock) {
		this(
			maxConcurrentCheckpointAttempts,
			rescheduleTrigger,
			clock,
			minPauseBetweenCheckpoints,
			pendingCheckpointsSizeSupplier,
			lock,
			DEFAULT_MAX_QUEUED_REQUESTS
		);
	}

	CheckpointRequestDecider(
			int maxConcurrentCheckpointAttempts,
			Consumer<Long> rescheduleTrigger,
			Clock clock,
			long minPauseBetweenCheckpoints,
			Supplier<Integer> pendingCheckpointsSizeSupplier,
			Object lock,
			int maxQueuedRequests) {
		Preconditions.checkArgument(maxConcurrentCheckpointAttempts > 0);
		Preconditions.checkArgument(maxQueuedRequests > 0);
		this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;
		this.rescheduleTrigger = rescheduleTrigger;
		this.clock = clock;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.pendingCheckpointsSizeSupplier = pendingCheckpointsSizeSupplier;
		this.lock = lock;
		this.maxQueuedRequests = maxQueuedRequests;
	}

	Optional<CheckpointTriggerRequest> chooseRequestToExecute(CheckpointTriggerRequest newRequest, boolean isTriggering, long lastCompletionMs) {
		synchronized (lock) {
			if (queuedRequests.size() >= maxQueuedRequests && !queuedRequests.last().isPeriodic) {
				// there are only non-periodic (ie user-submitted) requests enqueued - retain them and drop the new one
				newRequest.completeExceptionally(new CheckpointException(TOO_MANY_CHECKPOINT_REQUESTS));
				return Optional.empty();
			} else {
				queuedRequests.add(newRequest);
				if (queuedRequests.size() > maxQueuedRequests) {
					queuedRequests.pollLast().completeExceptionally(new CheckpointException(TOO_MANY_CHECKPOINT_REQUESTS));
				}
				Optional<CheckpointTriggerRequest> request = chooseRequestToExecute(isTriggering, lastCompletionMs);
				request.ifPresent(CheckpointRequestDecider::logInQueueTime);
				return request;
			}
		}
	}

	Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute(boolean isTriggering, long lastCompletionMs) {
		synchronized (lock) {
			Optional<CheckpointTriggerRequest> request = chooseRequestToExecute(isTriggering, lastCompletionMs);
			request.ifPresent(CheckpointRequestDecider::logInQueueTime);
			return request;
		}
	}

	/**
	 * Choose the next {@link CheckpointTriggerRequest request} to execute based on the provided candidate and the
	 * current state. Acquires a lock and may update the state.
	 * @return request to execute, if any.
	 */
	private Optional<CheckpointTriggerRequest> chooseRequestToExecute(boolean isTriggering, long lastCompletionMs) {
		Preconditions.checkState(Thread.holdsLock(lock));
		if (isTriggering || queuedRequests.isEmpty()) {
			return Optional.empty();
		}

		if (pendingCheckpointsSizeSupplier.get() >= maxConcurrentCheckpointAttempts) {
			return Optional.of(queuedRequests.first())
				.filter(CheckpointTriggerRequest::isForce)
				.map(unused -> queuedRequests.pollFirst());
		}

		long nextTriggerDelayMillis = nextTriggerDelayMillis(lastCompletionMs);
		if (nextTriggerDelayMillis > 0) {
			return onTooEarly(nextTriggerDelayMillis);
		}

		return Optional.of(queuedRequests.pollFirst());
	}

	private Optional<CheckpointTriggerRequest> onTooEarly(long nextTriggerDelayMillis) {
		CheckpointTriggerRequest first = queuedRequests.first();
		if (first.isForce()) {
			return Optional.of(queuedRequests.pollFirst());
		} else if (first.isPeriodic) {
			queuedRequests.pollFirst().completeExceptionally(new CheckpointException(MINIMUM_TIME_BETWEEN_CHECKPOINTS));
			rescheduleTrigger.accept(nextTriggerDelayMillis);
			return Optional.empty();
		} else {
			return Optional.empty();
		}
	}

	private long nextTriggerDelayMillis(long lastCheckpointCompletionRelativeTime) {
		return lastCheckpointCompletionRelativeTime - clock.relativeTimeMillis() + minPauseBetweenCheckpoints;
	}

	@VisibleForTesting
	@Deprecated
	PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
		synchronized (lock) {
			return new PriorityQueue<>(queuedRequests);
		}
	}

	void abortAll(CheckpointException exception) {
		Preconditions.checkState(Thread.holdsLock(lock));
		while (!queuedRequests.isEmpty()) {
			queuedRequests.pollFirst().completeExceptionally(exception);
		}
	}

	int getNumQueuedRequests() {
		synchronized (lock) {
			return queuedRequests.size();
		}
	}

	private static Comparator<CheckpointTriggerRequest> checkpointTriggerRequestsComparator() {
		return (r1, r2) -> {
			if (r1.props.isSavepoint() != r2.props.isSavepoint()) {
				return r1.props.isSavepoint() ? -1 : 1;
			} else if (r1.isForce() != r2.isForce()) {
				return r1.isForce() ? -1 : 1;
			} else if (r1.isPeriodic != r2.isPeriodic) {
				return r1.isPeriodic ? 1 : -1;
			} else if (r1.timestamp != r2.timestamp) {
				return Long.compare(r1.timestamp, r2.timestamp);
			} else {
				return Integer.compare(identityHashCode(r1), identityHashCode(r2));
			}
		};
	}

	private static void logInQueueTime(CheckpointTriggerRequest request) {
		if (LOG.isInfoEnabled()) {
			long timeInQueue = request.timestamp - currentTimeMillis();
			if (timeInQueue > LOG_TIME_IN_QUEUE_THRESHOLD_MS) {
				LOG.info("checkpoint request time in queue: {}", timeInQueue);
			}
		}
	}
}

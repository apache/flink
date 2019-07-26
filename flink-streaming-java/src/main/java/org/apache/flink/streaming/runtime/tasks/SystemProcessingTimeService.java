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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ProcessingTimeService} which assigns as current processing time the result of calling
 * {@link System#currentTimeMillis()} and registers timers using a {@link ScheduledThreadPoolExecutor}.
 */
public class SystemProcessingTimeService extends ProcessingTimeService {

	private static final Logger LOG = LoggerFactory.getLogger(SystemProcessingTimeService.class);

	private static final int STATUS_ALIVE = 0;
	private static final int STATUS_QUIESCED = 1;
	private static final int STATUS_SHUTDOWN = 2;

	// ------------------------------------------------------------------------

	/** The containing task that owns this time service provider. */
	private final AsyncExceptionHandler task;

	/** The lock that timers acquire upon triggering. */
	private final Object checkpointLock;

	/** The executor service that schedules and calls the triggers of this task. */
	private final ScheduledThreadPoolExecutor timerService;

	private final AtomicInteger status;

	public SystemProcessingTimeService(AsyncExceptionHandler failureHandler, Object checkpointLock) {
		this(failureHandler, checkpointLock, null);
	}

	public SystemProcessingTimeService(
			AsyncExceptionHandler task,
			Object checkpointLock,
			ThreadFactory threadFactory) {

		this.task = checkNotNull(task);
		this.checkpointLock = checkNotNull(checkpointLock);

		this.status = new AtomicInteger(STATUS_ALIVE);

		if (threadFactory == null) {
			this.timerService = new ScheduledThreadPoolExecutor(1);
		} else {
			this.timerService = new ScheduledThreadPoolExecutor(1, threadFactory);
		}

		// tasks should be removed if the future is canceled
		this.timerService.setRemoveOnCancelPolicy(true);

		// make sure shutdown removes all pending tasks
		this.timerService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.timerService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
	}

	@Override
	public long getCurrentProcessingTime() {
		return System.currentTimeMillis();
	}

	/**
	 * Registers a task to be executed no sooner than time {@code timestamp}, but without strong
	 * guarantees of order.
	 *
	 * @param timestamp Time when the task is to be enabled (in processing time)
	 * @param target    The task to be executed
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 */
	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {

		// delay the firing of the timer by 1 ms to align the semantics with watermark. A watermark
		// T says we won't see elements in the future with a timestamp smaller or equal to T.
		// With processing time, we therefore need to delay firing the timer by one ms.
		long delay = Math.max(timestamp - getCurrentProcessingTime(), 0) + 1;

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return timerService.schedule(
					new TriggerTask(status, task, checkpointLock, target, timestamp), delay, TimeUnit.MILLISECONDS);
		}
		catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(delay);
			}
			else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			}
			else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		long nextTimestamp = getCurrentProcessingTime() + initialDelay;

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return timerService.scheduleAtFixedRate(
				new RepeatedTriggerTask(status, task, checkpointLock, callback, nextTimestamp, period),
				initialDelay,
				period,
				TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(initialDelay);
			}
			else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			}
			else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	/**
	 * @return {@code true} is the status of the service
	 * is {@link #STATUS_ALIVE}, {@code false} otherwise.
	 */
	@VisibleForTesting
	boolean isAlive() {
		return status.get() == STATUS_ALIVE;
	}

	@Override
	public boolean isTerminated() {
		return status.get() == STATUS_SHUTDOWN;
	}

	@Override
	public void quiesce() throws InterruptedException {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_QUIESCED)) {
			timerService.shutdown();
		}
	}

	@Override
	public void awaitPendingAfterQuiesce() throws InterruptedException {
		if (!timerService.isTerminated()) {
			Preconditions.checkState(timerService.isTerminating() || timerService.isShutdown());

			// await forever (almost)
			timerService.awaitTermination(365L, TimeUnit.DAYS);
		}
	}

	@Override
	public void shutdownService() {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_SHUTDOWN) ||
				status.compareAndSet(STATUS_QUIESCED, STATUS_SHUTDOWN)) {
			timerService.shutdownNow();
		}
	}

	@Override
	public boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException {
		shutdownService();
		return timerService.awaitTermination(time, timeUnit);
	}

	@Override
	public boolean shutdownServiceUninterruptible(long timeoutMs) {

		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(timeoutMs));

		boolean shutdownComplete = false;
		boolean receivedInterrupt = false;

		do {
			try {
				// wait for a reasonable time for all pending timer threads to finish
				shutdownComplete = shutdownAndAwaitPending(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException iex) {
				receivedInterrupt = true;
				LOG.trace("Intercepted attempt to interrupt timer service shutdown.", iex);
			}
		} while (deadline.hasTimeLeft() && !shutdownComplete);

		if (receivedInterrupt) {
			Thread.currentThread().interrupt();
		}

		return shutdownComplete;
	}

	// safety net to destroy the thread pool
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		timerService.shutdownNow();
	}

	@VisibleForTesting
	int getNumTasksScheduled() {
		BlockingQueue<?> queue = timerService.getQueue();
		if (queue == null) {
			return 0;
		} else {
			return queue.size();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Internal task that is invoked by the timer service and triggers the target.
	 */
	private static final class TriggerTask implements Runnable {

		private final AtomicInteger serviceStatus;
		private final Object lock;
		private final ProcessingTimeCallback target;
		private final long timestamp;
		private final AsyncExceptionHandler exceptionHandler;

		private TriggerTask(
				final AtomicInteger serviceStatus,
				final AsyncExceptionHandler exceptionHandler,
				final Object lock,
				final ProcessingTimeCallback target,
				final long timestamp) {

			this.serviceStatus = Preconditions.checkNotNull(serviceStatus);
			this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler);
			this.lock = Preconditions.checkNotNull(lock);
			this.target = Preconditions.checkNotNull(target);
			this.timestamp = timestamp;
		}

		@Override
		public void run() {
			synchronized (lock) {
				try {
					if (serviceStatus.get() == STATUS_ALIVE) {
						target.onProcessingTime(timestamp);
					}
				} catch (Throwable t) {
					TimerException asyncException = new TimerException(t);
					exceptionHandler.handleAsyncException("Caught exception while processing timer.", asyncException);
				}
			}
		}
	}

	/**
	 * Internal task which is repeatedly called by the processing time service.
	 */
	private static final class RepeatedTriggerTask implements Runnable {

		private final AtomicInteger serviceStatus;
		private final Object lock;
		private final ProcessingTimeCallback target;
		private final long period;
		private final AsyncExceptionHandler exceptionHandler;

		private long nextTimestamp;

		private RepeatedTriggerTask(
				final AtomicInteger serviceStatus,
				final AsyncExceptionHandler exceptionHandler,
				final Object lock,
				final ProcessingTimeCallback target,
				final long nextTimestamp,
				final long period) {

			this.serviceStatus = Preconditions.checkNotNull(serviceStatus);
			this.lock = Preconditions.checkNotNull(lock);
			this.target = Preconditions.checkNotNull(target);
			this.period = period;
			this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler);

			this.nextTimestamp = nextTimestamp;
		}

		@Override
		public void run() {
			synchronized (lock) {
				try {
					if (serviceStatus.get() == STATUS_ALIVE) {
						target.onProcessingTime(nextTimestamp);
					}

					nextTimestamp += period;
				} catch (Throwable t) {
					TimerException asyncException = new TimerException(t);
					exceptionHandler.handleAsyncException("Caught exception while processing repeated timer task.", asyncException);
				}
			}
		}
	}
}

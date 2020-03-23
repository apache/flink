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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TimerService} which assigns as current processing time the result of calling
 * {@link System#currentTimeMillis()} and registers timers using a {@link ScheduledThreadPoolExecutor}.
 */
@Internal
public class SystemProcessingTimeService implements TimerService {

	private static final Logger LOG = LoggerFactory.getLogger(SystemProcessingTimeService.class);

	private static final int STATUS_ALIVE = 0;
	private static final int STATUS_QUIESCED = 1;
	private static final int STATUS_SHUTDOWN = 2;

	// ------------------------------------------------------------------------

	/** The executor service that schedules and calls the triggers of this task. */
	private final ScheduledThreadPoolExecutor timerService;

	private final ExceptionHandler exceptionHandler;
	private final AtomicInteger status;

	private final CompletableFuture<Void> quiesceCompletedFuture;

	@VisibleForTesting
	SystemProcessingTimeService(ExceptionHandler exceptionHandler) {
		this(exceptionHandler, null);
	}

	SystemProcessingTimeService(ExceptionHandler exceptionHandler, ThreadFactory threadFactory) {

		this.exceptionHandler = checkNotNull(exceptionHandler);
		this.status = new AtomicInteger(STATUS_ALIVE);
		this.quiesceCompletedFuture = new CompletableFuture<>();

		if (threadFactory == null) {
			this.timerService = new ScheduledTaskExecutor(1);
		} else {
			this.timerService = new ScheduledTaskExecutor(1, threadFactory);
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
	 * @param callback    The task to be executed
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 */
	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {

		long delay = ProcessingTimeServiceUtil.getProcessingTimeDelay(timestamp, getCurrentProcessingTime());

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return timerService.schedule(wrapOnTimerCallback(callback, timestamp), delay, TimeUnit.MILLISECONDS);
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
				wrapOnTimerCallback(callback, nextTimestamp, period),
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
	public CompletableFuture<Void> quiesce() {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_QUIESCED)) {
			timerService.shutdown();
		}

		return quiesceCompletedFuture;
	}

	@Override
	public void shutdownService() {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_SHUTDOWN) ||
				status.compareAndSet(STATUS_QUIESCED, STATUS_SHUTDOWN)) {
			timerService.shutdownNow();
		}
	}

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does wait
	 * for all timers to complete or until the time limit is exceeded. Any call to
	 * {@link #registerTimer(long, ProcessingTimeCallback)} will result in a hard exception after calling this method.
	 * @param time time to wait for termination.
	 * @param timeUnit time unit of parameter time.
	 * @return {@code true} if this timer service and all pending timers are terminated and
	 *         {@code false} if the timeout elapsed before this happened.
	 */
	@VisibleForTesting
	boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException {
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

	private class ScheduledTaskExecutor extends ScheduledThreadPoolExecutor {

		public ScheduledTaskExecutor(int corePoolSize) {
			super(corePoolSize);
		}

		public ScheduledTaskExecutor(int corePoolSize, ThreadFactory threadFactory) {
			super(corePoolSize, threadFactory);
		}

		@Override
		protected void terminated() {
			super.terminated();
			quiesceCompletedFuture.complete(null);
		}
	}

	/**
	 * An exception handler, called when {@link ProcessingTimeCallback} throws an exception.
	 */
	interface ExceptionHandler {
		void handleException(Exception ex);
	}

	private Runnable wrapOnTimerCallback(ProcessingTimeCallback callback, long timestamp) {
		return new ScheduledTask(status, exceptionHandler, callback, timestamp, 0);
	}

	private Runnable wrapOnTimerCallback(ProcessingTimeCallback callback, long nextTimestamp, long period) {
		return new ScheduledTask(status, exceptionHandler, callback, nextTimestamp, period);
	}

	private static final class ScheduledTask implements Runnable {
		private final AtomicInteger serviceStatus;
		private final ExceptionHandler exceptionHandler;
		private final ProcessingTimeCallback callback;

		private long nextTimestamp;
		private final long period;

		ScheduledTask(
				AtomicInteger serviceStatus,
				ExceptionHandler exceptionHandler,
				ProcessingTimeCallback callback,
				long timestamp,
				long period) {
			this.serviceStatus = serviceStatus;
			this.exceptionHandler = exceptionHandler;
			this.callback = callback;
			this.nextTimestamp = timestamp;
			this.period = period;
		}

		@Override
		public void run() {
			if (serviceStatus.get() != STATUS_ALIVE) {
				return;
			}
			try {
				callback.onProcessingTime(nextTimestamp);
			} catch (Exception ex) {
				exceptionHandler.handleException(ex);
			}
			nextTimestamp += period;
		}
	}
}

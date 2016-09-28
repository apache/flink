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
import org.apache.flink.streaming.runtime.operators.Triggerable;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TimeServiceProvider} which assigns as current processing time the result of calling
 * {@link System#currentTimeMillis()} and registers timers using a {@link ScheduledThreadPoolExecutor}.
 */
public class DefaultTimeServiceProvider extends TimeServiceProvider {

	/** The containing task that owns this time service provider. */
	private final AsyncExceptionHandler task;

	private final Object checkpointLock;

	/** The executor service that schedules and calls the triggers of this task*/
	private final ScheduledExecutorService timerService;

	public static DefaultTimeServiceProvider create(
			AsyncExceptionHandler exceptionHandler,
			ScheduledExecutorService executor,
			Object checkpointLock) {
		return new DefaultTimeServiceProvider(exceptionHandler, executor, checkpointLock);
	}

	private DefaultTimeServiceProvider(AsyncExceptionHandler task,
									ScheduledExecutorService threadPoolExecutor,
									Object checkpointLock) {
		this.task = task;
		this.timerService = threadPoolExecutor;
		this.checkpointLock = checkpointLock;
	}

	@Override
	public long getCurrentProcessingTime() {
		return System.currentTimeMillis();
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, Triggerable target) {
		long delay = Math.max(timestamp - getCurrentProcessingTime(), 0);
		return timerService.schedule(new TriggerTask(task, checkpointLock, target, timestamp), delay, TimeUnit.MILLISECONDS);
	}

	@Override
	public boolean isTerminated() {
		return timerService.isTerminated();
	}

	@Override
	public void shutdownService() throws Exception {
		timerService.shutdownNow();
	}

	/**
	 * Internal task that is invoked by the timer service and triggers the target.
	 */
	private static final class TriggerTask implements Runnable {

		private final Object lock;
		private final Triggerable target;
		private final long timestamp;
		private final AsyncExceptionHandler exceptionHandler;

		TriggerTask(AsyncExceptionHandler exceptionHandler, final Object lock, Triggerable target, long timestamp) {
			this.exceptionHandler = exceptionHandler;
			this.lock = lock;
			this.target = target;
			this.timestamp = timestamp;
		}

		@Override
		public void run() {
			synchronized (lock) {
				try {
					target.trigger(timestamp);
				} catch (Throwable t) {
					TimerException asyncException = new TimerException(t);
					exceptionHandler.registerAsyncException(asyncException);
				}
			}
		}
	}

	@VisibleForTesting
	public static DefaultTimeServiceProvider createForTesting(ScheduledExecutorService executor, Object checkpointLock) {
		return new DefaultTimeServiceProvider(new AsyncExceptionHandler() {
			@Override
			public void registerAsyncException(AsynchronousException exception) {
				exception.printStackTrace();
			}
		}, executor, checkpointLock);
	}
}

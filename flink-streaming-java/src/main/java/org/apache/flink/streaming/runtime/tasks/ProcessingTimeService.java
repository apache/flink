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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Defines the current processing time and handles all related actions,
 * such as register timers for tasks to be executed in the future.
 *
 * <p>The access to the time via {@link #getCurrentProcessingTime()} is always available, regardless of
 * whether the timer service has been shut down.
 *
 * <p>The registration of timers follows a life cycle of three phases:
 * <ol>
 *     <li>In the initial state, it accepts timer registrations and triggers when the time is reached.</li>
 *     <li>After calling {@link #quiesce()}, further calls to
 *         {@link #registerTimer(long, ProcessingTimeCallback)} will not register any further timers, and will
 *         return a "dummy" future as a result. This is used for clean shutdown, where currently firing
 *         timers are waited for and no future timers can be scheduled, without causing hard exceptions.</li>
 *     <li>After a call to {@link #shutdownService()}, all calls to {@link #registerTimer(long, ProcessingTimeCallback)}
 *         will result in a hard exception.</li>
 * </ol>
 */
public abstract class ProcessingTimeService {

	/**
	 * Returns the current processing time.
	 */
	public abstract long getCurrentProcessingTime();

	/**
	 * Registers a task to be executed when (processing) time is {@code timestamp}.
	 *
	 * @param timestamp   Time when the task is to be executed (in processing time)
	 * @param target      The task to be executed
	 *
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 */
	public abstract ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target);

	/**
	 * Registers a task to be executed repeatedly at a fixed rate.
	 *
	 * @param callback to be executed after the initial delay and then after each period
	 * @param initialDelay initial delay to start executing callback
	 * @param period after the initial delay after which the callback is executed
	 * @return Scheduled future representing the task to be executed repeatedly
	 */
	public abstract ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period);

	/**
	 * Returns <tt>true</tt> if the service has been shut down, <tt>false</tt> otherwise.
	 */
	public abstract boolean isTerminated();

	/**
	 * This method puts the service into a state where it does not register new timers, but
	 * returns for each call to {@link #registerTimer(long, ProcessingTimeCallback)} only a "mock" future.
	 * Furthermore, the method clears all not yet started timers.
	 *
	 * <p>This method can be used to cleanly shut down the timer service. The using components
	 * will not notice that the service is shut down (as for example via exceptions when registering
	 * a new timer), but the service will simply not fire any timer any more.
	 */
	public abstract void quiesce() throws InterruptedException;

	/**
	 * This method can be used after calling {@link #quiesce()}, and awaits the completion
	 * of currently executing timers.
	 */
	public abstract void awaitPendingAfterQuiesce() throws InterruptedException;

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does not wait
	 * for any timer to complete. Any further call to {@link #registerTimer(long, ProcessingTimeCallback)}
	 * will result in a hard exception.
	 */
	public abstract void shutdownService();

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does not wait
	 * for any timer to complete. Any further call to {@link #registerTimer(long, ProcessingTimeCallback)}
	 * will result in a hard exception. This call cannot be interrupted and will block until the shutdown is completed
	 * or the timeout is exceeded.
	 *
	 * @param timeoutMs timeout for blocking on the service shutdown in milliseconds.
	 * @return returns true iff the shutdown was completed.
	 */
	public abstract boolean shutdownServiceUninterruptible(long timeoutMs);

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does wait
	 * for all timers to complete or until the time limit is exceeded. Any call to
	 * {@link #registerTimer(long, ProcessingTimeCallback)} will result in a hard exception after calling this method.
	 * @param time time to wait for termination.
	 * @param timeUnit time unit of parameter time.
	 * @return {@code true} if this timer service and all pending timers are terminated and
	 *         {@code false} if the timeout elapsed before this happened.
	 */
	public abstract boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException;
}

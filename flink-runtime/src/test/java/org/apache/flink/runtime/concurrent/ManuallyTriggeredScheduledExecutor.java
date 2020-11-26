/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.concurrent;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Simple {@link ScheduledExecutor} implementation for testing purposes.
 */
public class ManuallyTriggeredScheduledExecutor implements ScheduledExecutor {

	/**
	 * The service that we redirect to. We wrap this rather than extending it to limit the
	 * surfaced interface.
	 */
	org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService execService =
			new org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService();

	@Override
	public void execute(@Nonnull Runnable command) {
		execService.execute(command);
	}

	/** Triggers all {@code queuedRunnables}. */
	public void triggerAll() {
		execService.triggerAll();
	}

	/**
	 * Triggers the next queued runnable and executes it synchronously.
	 * This method throws an exception if no Runnable is currently queued.
	 */
	public void trigger() {
		execService.trigger();
	}

	/**
	 * Gets the number of Runnables currently queued.
	 */
	public int numQueuedRunnables() {
		return execService.numQueuedRunnables();
	}

	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		return execService.schedule(command, delay, unit);
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		return execService.schedule(callable, delay, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		return execService.scheduleAtFixedRate(command, initialDelay, period, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		return execService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}

	public Collection<ScheduledFuture<?>> getScheduledTasks() {
		return execService.getScheduledTasks();
	}

	public Collection<ScheduledFuture<?>> getPeriodicScheduledTask() {
		return execService.getPeriodicScheduledTask();
	}

	public Collection<ScheduledFuture<?>> getNonPeriodicScheduledTask() {
		return execService.getNonPeriodicScheduledTask();
	}

	/**
	 * Triggers all registered tasks.
	 */
	public void triggerScheduledTasks() {
		execService.triggerScheduledTasks();
	}

	/**
	 * Triggers a single non-periodically scheduled task.
	 *
	 * @throws NoSuchElementException If there is no such task.
	 */
	public void triggerNonPeriodicScheduledTask() {
		execService.triggerNonPeriodicScheduledTask();
	}

	/**
	 * Triggers all non-periodically scheduled tasks. In contrast to {@link #triggerNonPeriodicScheduledTasks()},
	 * if such a task schedules another non-periodically schedule task, then this new task will also be triggered.
	 */
	public void triggerNonPeriodicScheduledTasksWithRecursion() {
		execService.triggerNonPeriodicScheduledTasksWithRecursion();
	}

	public void triggerNonPeriodicScheduledTasks() {
		execService.triggerNonPeriodicScheduledTasks();
	}

	public void triggerPeriodicScheduledTasks() {
		execService.triggerPeriodicScheduledTasks();
	}
}

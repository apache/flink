/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.restart.RestartCallback;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;

import javax.annotation.Nonnull;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A {@link RestartStrategy} for tests that gives fine-grained control over the point in time when restart actions are
 * performed.
 */
public class TestRestartStrategy implements RestartStrategy {

	@Nonnull
	private final Queue<ExecutorAction> actionsQueue;

	private final int maxRestarts;

	private int restartAttempts;

	private boolean manuallyTriggeredExecution;

	public TestRestartStrategy() {
		this(true);
	}

	public TestRestartStrategy(boolean manuallyTriggeredExecution) {
		this(-1, manuallyTriggeredExecution);
	}

	public TestRestartStrategy(int maxRestarts, boolean manuallyTriggeredExecution) {
		this(new LinkedList<>(), maxRestarts, manuallyTriggeredExecution);
	}

	public TestRestartStrategy(
		@Nonnull Queue<ExecutorAction> actionsQueue,
		int maxRestarts,
		boolean manuallyTriggeredExecution) {

		this.actionsQueue = actionsQueue;
		this.maxRestarts = maxRestarts;
		this.manuallyTriggeredExecution = manuallyTriggeredExecution;
	}

	@Override
	public boolean canRestart() {
		return maxRestarts < 0 || maxRestarts - restartAttempts > 0;
	}

	@Override
	public CompletableFuture<Void> restart(RestartCallback restarter, ScheduledExecutor executor) {

		++restartAttempts;
		ExecutorAction executorAction = new ExecutorAction(restarter::triggerFullRecovery, executor);
		if (manuallyTriggeredExecution) {
			synchronized (actionsQueue) {
				actionsQueue.add(executorAction);
			}
			return new CompletableFuture<>();
		} else {
			return executorAction.trigger();
		}
	}

	public int getNumberOfQueuedActions() {
		synchronized (actionsQueue) {
			return actionsQueue.size();
		}
	}

	public CompletableFuture<Void> triggerNextAction() {
		synchronized (actionsQueue) {
			return actionsQueue.remove().trigger();
		}
	}

	public CompletableFuture<Void> triggerAll() {

		synchronized (actionsQueue) {

			if (actionsQueue.isEmpty()) {
				return CompletableFuture.completedFuture(null);
			}

			CompletableFuture<?>[] completableFutures = new CompletableFuture[actionsQueue.size()];
			for (int i = 0; i < completableFutures.length; ++i) {
				completableFutures[i] = triggerNextAction();
			}
			return CompletableFuture.allOf(completableFutures);
		}
	}

	public boolean isManuallyTriggeredExecution() {
		return manuallyTriggeredExecution;
	}

	public void setManuallyTriggeredExecution(boolean manuallyTriggeredExecution) {
		this.manuallyTriggeredExecution = manuallyTriggeredExecution;
	}

	public static TestRestartStrategy manuallyTriggered() {
		return new TestRestartStrategy(true);
	}

	public static TestRestartStrategy directExecuting() {
		return new TestRestartStrategy(false);
	}

	private static class ExecutorAction {

		final Runnable runnable;
		final Executor executor;

		ExecutorAction(Runnable runnable, Executor executor) {
			this.runnable = runnable;
			this.executor = executor;
		}

		public CompletableFuture<Void> trigger() {
			return CompletableFuture.runAsync(runnable, executor);
		}
	}
}

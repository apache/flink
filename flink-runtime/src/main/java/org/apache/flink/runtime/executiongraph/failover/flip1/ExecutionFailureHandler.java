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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.throwable.ThrowableClassifier;
import org.apache.flink.runtime.throwable.ThrowableType;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This handler deals with task failures to return a {@link FailureHandlingResult} which contains tasks
 * to restart to recover from failures.
 */
public class ExecutionFailureHandler {

	/** Strategy to judge which tasks should be restarted. */
	private FailoverStrategy failoverStrategy;

	/** Strategy to judge whether and when a restarting should be done. */
	private RestartBackoffTimeStrategy restartBackoffTimeStrategy;

	/**
	 * Creates the handler to deal with task failures.
	 *
	 * @param topology of tasks of current job
	 * @param failoverStrategyFactory is the factory to create the failover strategy
	 * @param restartStrategyFactory is the factory to create the restart strategy
	 */
	public ExecutionFailureHandler(
		FailoverTopology topology,
		FailoverStrategy.Factory failoverStrategyFactory,
		RestartBackoffTimeStrategy.Factory restartStrategyFactory) {

		this.failoverStrategy = checkNotNull(failoverStrategyFactory.create(topology));
		this.restartBackoffTimeStrategy = checkNotNull(restartStrategyFactory.create());
	}

	/**
	 * Return result of failure handling. Can be a set of task vertices to restart
	 * and a delay of the restarting. Or that the failure is not recoverable and the reason for it.
	 *
	 * @param failedTask is the ID of the failed task vertex
	 * @param cause of the task failure
	 * @return result of the failure handling
	 */
	public FailureHandlingResult getFailureHandlingResult(ExecutionVertexID failedTask, Throwable cause) {
		if (ThrowableClassifier.getThrowableType(cause) == ThrowableType.NonRecoverableError) {
			return new FailureHandlingResult(new JobException("The failure is not recoverable", cause));
		}

		restartBackoffTimeStrategy.notifyFailure(cause);
		if (restartBackoffTimeStrategy.canRestart()) {
			return new FailureHandlingResult(
				failoverStrategy.getTasksNeedingRestart(failedTask, cause),
				restartBackoffTimeStrategy.getBackoffTime());
		} else {
			return new FailureHandlingResult(
				new JobException("Failed task restarting is suppressed by " + restartBackoffTimeStrategy, cause));
		}
	}

	@VisibleForTesting
	FailoverStrategy getFailoverStrategy() {
		return failoverStrategy;
	}

	@VisibleForTesting
	RestartBackoffTimeStrategy getRestartBackoffTimeStrategy() {
		return restartBackoffTimeStrategy;
	}
}

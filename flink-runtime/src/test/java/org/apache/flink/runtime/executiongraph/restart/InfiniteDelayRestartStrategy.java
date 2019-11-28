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

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Testing restart strategy which promise to restart {@link ExecutionGraph} after the infinite time delay.
 * Actually {@link ExecutionGraph} will never be restarted. No additional threads will be used.
 */
public class InfiniteDelayRestartStrategy implements RestartStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(InfiniteDelayRestartStrategy.class);

	private final int maxRestartAttempts;
	private int restartAttemptCounter;

	public InfiniteDelayRestartStrategy() {
		this(-1);
	}

	public InfiniteDelayRestartStrategy(int maxRestartAttempts) {
		this.maxRestartAttempts = maxRestartAttempts;
		restartAttemptCounter = 0;
	}

	@Override
	public boolean canRestart() {
		return maxRestartAttempts < 0 || restartAttemptCounter < maxRestartAttempts;
	}

	@Override
	public CompletableFuture<Void> restart(RestartCallback restarter, ScheduledExecutor executor) {
		LOG.info("Delaying retry of job execution forever");

		if (maxRestartAttempts >= 0) {
			restartAttemptCounter++;
		}
		return new CompletableFuture<>();
	}
}

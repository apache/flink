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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class ExecutionGraphRestarter {
	private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraphRestarter.class);

	/** @deprecated Using {@link #scheduleRestartWithDelay(ExecutionGraph, long, ScheduledExecutorService)} instead. */
	@Deprecated
	public static Callable<Object> restartWithDelay(final ExecutionGraph executionGraph, final long delayBetweenRestartAttemptsInMillis) {
		return new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				try {
					LOG.info("Delaying retry of job execution for {} ms ...", delayBetweenRestartAttemptsInMillis);
					// do the delay
					Thread.sleep(delayBetweenRestartAttemptsInMillis);
				} catch(InterruptedException e) {
					// should only happen on shutdown
				}
				executionGraph.restart();
				return null;
			}
		};
	}

	public static void scheduleRestartWithDelay(final ExecutionGraph executionGraph, final long delayBetweenRestartAttemptsInMillis,
												final ScheduledExecutorService executorService) {
		ScheduledFuture<Object> future = executorService.schedule(
			new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					executionGraph.restart();
					return null;
				}
			}, delayBetweenRestartAttemptsInMillis, TimeUnit.MILLISECONDS
		);

		try {
			future.get();
		} catch (Throwable throwable) {
			LOG.warn("Uncaught exception while restarting job " + executionGraph.getJobName(), throwable);
		}
	}
}

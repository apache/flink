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

package org.apache.flink.runtime.executiongraph.metrics;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A gauge that returns (in milliseconds) how long a job has not been not running any
 * more, in case it is in a failing/recovering situation. Running jobs return naturally
 * a value of zero.
 *
 * <p>For jobs that have never run (new not yet scheduled jobs) or jobs that
 * have run again after failing, this gauge returns {@value NOT_FAILING},
 * and for jobs that are not running any more, it returns {@value NO_LONGER_RUNNING}.
 */
public class DownTimeGauge implements Gauge<Long> {

	public static final String METRIC_NAME = "downtime";

	private static final long NOT_FAILING = 0L;

	private static final long NO_LONGER_RUNNING = -1L;

	// ------------------------------------------------------------------------

	private final ExecutionGraph eg;

	public DownTimeGauge(ExecutionGraph executionGraph) {
		this.eg = checkNotNull(executionGraph);
	}

	// ------------------------------------------------------------------------

	@Override
	public Long getValue() {
		final JobStatus status = eg.getState();

		// not running any more -> finished or not on leader
		if (status.isTerminalState()) {
			return NO_LONGER_RUNNING;
		}

		final long runningTimestamp = eg.getStatusTimestamp(JobStatus.RUNNING);
		final long failingTimestamp = eg.getStatusTimestamp(JobStatus.FAILING);

		if (failingTimestamp <= runningTimestamp) {
			return NOT_FAILING;
		} else {
			// we use 'Math.max' here to avoid negative timestamps when clocks change
			return Math.max(System.currentTimeMillis() - failingTimestamp, 0);
		}
	}
}

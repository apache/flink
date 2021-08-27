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
import org.apache.flink.runtime.executiongraph.JobStatusProvider;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A gauge that returns (in milliseconds) how long a job to get to running.
 *
 * <p>For jobs that are not running any more, it returns {@value NO_LONGER_RUNNING}.
 */
public class StartingTimeGauge implements Gauge<Long> {

    public static final String METRIC_NAME = "startingTime";

    private static final long NOT_RUNNING = 0L;

    private static final long NO_LONGER_RUNNING = -1L;

    // ------------------------------------------------------------------------

    private final JobStatusProvider jobStatusProvider;

    public StartingTimeGauge(JobStatusProvider jobStatusProvider) {
        this.jobStatusProvider = checkNotNull(jobStatusProvider);
    }

    // ------------------------------------------------------------------------

    @Override
    public Long getValue() {
        final JobStatus status = jobStatusProvider.getState();

        // not running any more
        if (status.isTerminalState()) {
            return NO_LONGER_RUNNING;
        }

        final long createdTimestamp = jobStatusProvider.getStatusTimestamp(JobStatus.CREATED);
        final long runningTimestamp = jobStatusProvider.getStatusTimestamp(JobStatus.RUNNING);

        if (runningTimestamp <= createdTimestamp) {
            // Job not running
            return NOT_RUNNING;
        } else {
            // Job started, return the diff between RUNNING and CREATED
            return runningTimestamp - createdTimestamp;
        }
    }
}

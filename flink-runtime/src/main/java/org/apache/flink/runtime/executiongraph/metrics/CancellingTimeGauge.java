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

/** A gauge that returns (in milliseconds) how long a job spent in CANCELING state. */
public class CancellingTimeGauge implements Gauge<Long> {

    public static final String METRIC_NAME = "cancellingTime";

    private static final long NOT_CANCELLING = 0L;

    // ------------------------------------------------------------------------

    private final JobStatusProvider jobStatusProvider;

    public CancellingTimeGauge(JobStatusProvider jobStatusProvider) {
        this.jobStatusProvider = checkNotNull(jobStatusProvider);
    }

    // ------------------------------------------------------------------------

    @Override
    public Long getValue() {
        final JobStatus status = jobStatusProvider.getState();

        final long cancellingTimestamp = jobStatusProvider.getStatusTimestamp(JobStatus.CANCELLING);
        final long canceledTimestamp = jobStatusProvider.getStatusTimestamp(JobStatus.CANCELED);

        if (cancellingTimestamp <= 0L) {
            // Job is not in CANCELLING or CANCELED state
            return NOT_CANCELLING;
        } else if (canceledTimestamp <= 0L) {
            // Job is in CANCELLING state but has not been canceled
            return Math.max(System.currentTimeMillis() - cancellingTimestamp, 0L);
        } else {
            // Job has been canceled
            return canceledTimestamp - cancellingTimestamp;
        }
    }
}

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

package org.apache.flink.connectors.test.common.utils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Helper components for checking Flink job status. */
public class FlinkJobStatusHelper {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJobStatusHelper.class);

    /**
     * Wait until the job enters expected status.
     *
     * @param client Client of the job
     * @param expectedStatus Expected job status
     */
    public static void waitForJobStatus(
            JobClient client, List<JobStatus> expectedStatus, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        LOG.debug("Waiting for job entering status {}...", expectedStatus);

        JobStatus status = null;
        while (status == null || !expectedStatus.contains(status)) {
            if (isTimeout(deadline)) {
                throw new TimeoutException(
                        String.format(
                                "Timeout waiting for job entering %s status. The last detected status was %s",
                                expectedStatus, status));
            }
            status = client.getJobStatus().get();
            if (status.isTerminalState()) {
                break;
            }
        }

        // If the job is entering an unexpected terminal status
        if (status.isTerminalState() && !expectedStatus.contains(status)) {
            try {
                // Exception will be exposed here if job failed
                client.getJobExecutionResult().get();
            } catch (Exception e) {
                throw new IllegalStateException(
                        String.format(
                                "Job has entered %s state, but expecting %s",
                                status, expectedStatus),
                        e);
            }
            throw new IllegalStateException(
                    String.format(
                            "Job has entered %s state, but expecting %s", status, expectedStatus));
        }
    }

    public static void terminateJob(JobClient client, Duration timeout) throws Exception {
        client.cancel().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static boolean isTimeout(long deadline) {
        return System.currentTimeMillis() > deadline;
    }
}

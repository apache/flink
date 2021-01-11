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

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/** Helper components for checking Flink job status. */
public class FlinkJobStatusHelper {

    /**
     * Wait until the job enters expected status.
     *
     * @param client Client of the job
     * @param expectedStatus Expected job status
     */
    public static void waitForJobStatus(
            JobClient client, JobStatus expectedStatus, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        JobStatus status = null;
        try {
            while (status == null || !status.equals(expectedStatus)) {
                if (isTimeout(deadline)) {
                    throw new TimeoutException(
                            String.format(
                                    "Timeout waiting for job entering %s status", expectedStatus));
                }
                status = client.getJobStatus().get();
                if (status.isTerminalState()) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to get status of the job", e);
        }

        // If the job is entering an unexpected terminal status
        if (status.isTerminalState() && !status.equals(expectedStatus)) {
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

    private static boolean isTimeout(long deadline) {
        return System.currentTimeMillis() > deadline;
    }
}

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

package org.apache.flink.client.deployment.cli;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for {@link org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer}.
 */
@ExtendWith(TestLoggerExtension.class)
public class ApplicationClusterDeployerTest {
    private static final JobID TESTING_JOB_ID = new JobID();

    @Test
    public void testWaitUtilTargetStatusReached_normal() throws JobExecutionException {
        ApplicationClusterDeployer.waitUtilTargetStatusReached(
                TESTING_JOB_ID, () -> JobStatus.FINISHED, this::isInTargetStatus);

        AtomicInteger attempt = new AtomicInteger();
        ApplicationClusterDeployer.waitUtilTargetStatusReached(
                TESTING_JOB_ID,
                () -> {
                    if (attempt.getAndIncrement() < 2) {
                        return JobStatus.RUNNING;
                    }
                    return JobStatus.FINISHED;
                },
                this::isInTargetStatus);
    }

    @Test
    public void testWaitUtilTargetStatusReached_executionFailed() {
        assertThrows(
                JobExecutionException.class,
                () ->
                        ApplicationClusterDeployer.waitUtilTargetStatusReached(
                                TESTING_JOB_ID, () -> JobStatus.FAILED, this::isInTargetStatus));
    }

    @Test
    public void testWaitUntilTargetStatusReached_statusFetchingError() {
        assertThrows(
                RuntimeException.class,
                () -> {
                    ApplicationClusterDeployer.waitUtilTargetStatusReached(
                            TESTING_JOB_ID,
                            () -> {
                                throw new Exception("Failed to get job status.");
                            },
                            this::isInTargetStatus);
                });
    }

    private boolean isInTargetStatus(JobStatus jobStatus) {
        return jobStatus == JobStatus.FINISHED || jobStatus == JobStatus.FAILED;
    }
}

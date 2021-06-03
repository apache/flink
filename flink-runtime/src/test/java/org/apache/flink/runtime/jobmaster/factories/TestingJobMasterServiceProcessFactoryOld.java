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

package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.DefaultJobMasterServiceProcess;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterService;
import org.apache.flink.runtime.jobmaster.JobMasterServiceProcess;
import org.apache.flink.runtime.jobmaster.TestingJobMasterService;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Testing implementation of the {@link JobMasterServiceProcessFactory} which returns a {@link
 * JobMaster} mock.
 */
public class TestingJobMasterServiceProcessFactoryOld implements JobMasterServiceProcessFactory {

    private final CompletableFuture<JobMasterService> jobMasterServiceFuture;
    private final JobID jobId;
    private final JobMasterServiceFactory jobMasterServiceFactoryNg;

    public TestingJobMasterServiceProcessFactoryOld(
            JobID jobId, CompletableFuture<JobMasterService> jobMasterServiceFuture) {
        this.jobId = jobId;
        this.jobMasterServiceFuture = jobMasterServiceFuture;
        this.jobMasterServiceFactoryNg =
                new TestingFutureJobMasterServiceFactory(jobMasterServiceFuture);
    }

    public TestingJobMasterServiceProcessFactoryOld(JobID jobId) {
        this(jobId, CompletableFuture.completedFuture(new TestingJobMasterService()));
    }

    @Override
    public JobMasterServiceProcess create(UUID leaderSessionID) {
        return new DefaultJobMasterServiceProcess(
                this.jobId,
                leaderSessionID,
                jobMasterServiceFactoryNg,
                cause -> createArchivedExecutionGraph(JobStatus.FAILED, cause));
    }

    @Override
    public JobID getJobId() {
        return this.jobId;
    }

    @Override
    public ArchivedExecutionGraph createArchivedExecutionGraph(
            JobStatus jobStatus, @Nullable Throwable cause) {
        return ArchivedExecutionGraph.createFromInitializingJob(
                jobId, "test-job", jobStatus, cause, null, System.currentTimeMillis());
    }

    public static class TestingFutureJobMasterServiceFactory implements JobMasterServiceFactory {

        final CompletableFuture<JobMasterService> jobMasterServiceFuture;

        public TestingFutureJobMasterServiceFactory(
                CompletableFuture<JobMasterService> jobMasterServiceFuture) {
            this.jobMasterServiceFuture = jobMasterServiceFuture;
        }

        @Override
        public CompletableFuture<JobMasterService> createJobMasterService(
                UUID leaderSessionId, OnCompletionActions onCompletionActions) {
            return jobMasterServiceFuture;
        }
    }
}

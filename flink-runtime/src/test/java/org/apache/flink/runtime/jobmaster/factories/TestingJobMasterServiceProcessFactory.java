/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.jobmaster.JobMasterServiceProcess;
import org.apache.flink.runtime.jobmaster.TestingJobMasterServiceProcess;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.function.Function;

/** Testing factory for {@link JobMasterServiceProcessFactory}. */
public class TestingJobMasterServiceProcessFactory implements JobMasterServiceProcessFactory {

    private final Function<UUID, JobMasterServiceProcess> jobMasterServiceProcessFunction;

    private final JobID jobId;

    private final String jobName;

    private final long initializationTimestamp;

    private TestingJobMasterServiceProcessFactory(
            Function<UUID, JobMasterServiceProcess> jobMasterServiceProcessFunction,
            JobID jobId,
            String jobName,
            long initializationTimestamp) {
        this.jobMasterServiceProcessFunction = jobMasterServiceProcessFunction;
        this.jobId = jobId;
        this.jobName = jobName;
        this.initializationTimestamp = initializationTimestamp;
    }

    @Override
    public JobMasterServiceProcess create(UUID leaderSessionId) {
        return jobMasterServiceProcessFunction.apply(leaderSessionId);
    }

    @Override
    public JobID getJobId() {
        return jobId;
    }

    @Override
    public ArchivedExecutionGraph createArchivedExecutionGraph(
            JobStatus jobStatus, @Nullable Throwable cause) {
        return ArchivedExecutionGraph.createFromInitializingJob(
                jobId, jobName, jobStatus, cause, null, initializationTimestamp);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Function<UUID, JobMasterServiceProcess> jobMasterServiceProcessFunction =
                ignored -> TestingJobMasterServiceProcess.newBuilder().build();
        private JobID jobId = new JobID();
        private String jobName = "foobar";
        private long initializationTimestamp = 1337L;

        public Builder setJobMasterServiceProcessFunction(
                Function<UUID, JobMasterServiceProcess> jobMasterServiceProcessFunction) {
            this.jobMasterServiceProcessFunction = jobMasterServiceProcessFunction;
            return this;
        }

        public Builder setJobId(JobID jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder setJobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        public Builder setInitializationTimestamp(long initializationTimestamp) {
            this.initializationTimestamp = initializationTimestamp;
            return this;
        }

        public TestingJobMasterServiceProcessFactory build() {
            return new TestingJobMasterServiceProcessFactory(
                    jobMasterServiceProcessFunction, jobId, jobName, initializationTimestamp);
        }
    }
}

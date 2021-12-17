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
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.DefaultJobMasterServiceProcess;
import org.apache.flink.runtime.jobmaster.JobMasterServiceProcess;

import javax.annotation.Nullable;

import java.util.UUID;

public class DefaultJobMasterServiceProcessFactory implements JobMasterServiceProcessFactory {

    private final JobID jobId;
    private final String jobName;
    @Nullable private final JobCheckpointingSettings checkpointingSettings;
    private final long initializationTimestamp;

    private final JobMasterServiceFactory jobMasterServiceFactory;

    public DefaultJobMasterServiceProcessFactory(
            JobID jobId,
            String jobName,
            @Nullable JobCheckpointingSettings checkpointingSettings,
            long initializationTimestamp,
            JobMasterServiceFactory jobMasterServiceFactory) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.checkpointingSettings = checkpointingSettings;
        this.initializationTimestamp = initializationTimestamp;
        this.jobMasterServiceFactory = jobMasterServiceFactory;
    }

    @Override
    public JobMasterServiceProcess create(UUID leaderSessionId) {
        return new DefaultJobMasterServiceProcess(
                jobId,
                leaderSessionId,
                jobMasterServiceFactory,
                cause -> createArchivedExecutionGraph(JobStatus.FAILED, cause));
    }

    @Override
    public JobID getJobId() {
        return jobId;
    }

    @Override
    public ArchivedExecutionGraph createArchivedExecutionGraph(
            JobStatus jobStatus, @Nullable Throwable cause) {
        return ArchivedExecutionGraph.createFromInitializingJob(
                jobId, jobName, jobStatus, cause, checkpointingSettings, initializationTimestamp);
    }
}

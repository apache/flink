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

package org.apache.flink.runtime.resourcemanager.registration;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.util.Preconditions;

/**
 * Container for JobManager related registration information, such as the leader id or the job id.
 */
public class JobManagerRegistration {
    private final JobID jobID;

    private final ResourceID jobManagerResourceID;

    private final JobMasterGateway jobManagerGateway;

    public JobManagerRegistration(
            JobID jobID, ResourceID jobManagerResourceID, JobMasterGateway jobManagerGateway) {
        this.jobID = Preconditions.checkNotNull(jobID);
        this.jobManagerResourceID = Preconditions.checkNotNull(jobManagerResourceID);
        this.jobManagerGateway = Preconditions.checkNotNull(jobManagerGateway);
    }

    public JobID getJobID() {
        return jobID;
    }

    public ResourceID getJobManagerResourceID() {
        return jobManagerResourceID;
    }

    public JobMasterId getJobMasterId() {
        return jobManagerGateway.getFencingToken();
    }

    public JobMasterGateway getJobManagerGateway() {
        return jobManagerGateway;
    }
}

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

package org.apache.flink.api.common;

import org.apache.flink.annotation.Internal;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The default implementation of {@link JobInfo}. */
@Internal
public class JobInfoImpl implements JobInfo {

    private final JobID jobID;

    private final String jobName;

    public JobInfoImpl(JobID jobID, String jobName) {
        checkArgument(jobID != null, "The job id must not be null.");
        checkArgument(jobName != null, "The job name must not be null.");
        this.jobID = jobID;
        this.jobName = jobName;
    }

    @Override
    public JobID getJobId() {
        return jobID;
    }

    @Override
    public String getJobName() {
        return jobName;
    }
}

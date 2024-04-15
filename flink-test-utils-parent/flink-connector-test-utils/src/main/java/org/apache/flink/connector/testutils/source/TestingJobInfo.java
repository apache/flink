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

package org.apache.flink.connector.testutils.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;

/** Test implementation for {@link JobInfo}. */
public class TestingJobInfo implements JobInfo {

    private final JobID jobID;

    private final String jobName;

    public TestingJobInfo(JobID jobID, String jobName) {
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

    /** Builder for {@link TestingJobInfo}. */
    public static class Builder {

        private JobID jobID = null;
        private String jobName = "";

        public Builder() {}

        public Builder setJobID(JobID jobID) {
            this.jobID = jobID;
            return this;
        }

        public Builder setJobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        public JobInfo build() {
            return new TestingJobInfo(jobID, jobName);
        }
    }
}

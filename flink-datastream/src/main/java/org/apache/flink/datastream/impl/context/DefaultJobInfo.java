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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.datastream.api.context.JobInfo;
import org.apache.flink.runtime.jobgraph.JobType;

/** Default implementation of {@link JobInfo}. */
public class DefaultJobInfo implements JobInfo {
    private final String jobName;

    private final JobType jobType;

    public DefaultJobInfo(String jobName, JobType jobType) {
        this.jobName = jobName;
        this.jobType = jobType;
    }

    @Override
    public String getJobName() {
        return jobName;
    }

    @Override
    public ExecutionMode getExecutionMode() {
        return jobType == JobType.STREAMING ? ExecutionMode.STREAMING : ExecutionMode.BATCH;
    }
}

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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The status of a specific job. */
public class JobStatusInfo implements ResponseBody, InfoMessage {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_STATUS = "status";

    @JsonProperty(FIELD_NAME_STATUS)
    private final JobStatus jobStatus;

    @JsonCreator
    public JobStatusInfo(@JsonProperty(FIELD_NAME_STATUS) JobStatus jobStatus) {
        this.jobStatus = checkNotNull(jobStatus);
    }

    @JsonIgnore
    public JobStatus getJobStatus() {
        return jobStatus;
    }

    @Override
    public int hashCode() {
        return jobStatus.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof JobStatusInfo) {
            JobStatusInfo that = (JobStatusInfo) obj;
            return jobStatus.equals(that.jobStatus);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "JobStatusInfo { " + jobStatus + " }";
    }
}

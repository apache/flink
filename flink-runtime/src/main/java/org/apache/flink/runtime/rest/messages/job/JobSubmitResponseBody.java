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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Response to the submission of a job, containing a URL from which the status of the job can be
 * retrieved from.
 */
public final class JobSubmitResponseBody implements ResponseBody {

    public static final String FIELD_NAME_JOB_URL = "jobUrl";

    /** The URL under which the job status can monitored. */
    @JsonProperty(FIELD_NAME_JOB_URL)
    public final String jobUrl;

    @JsonCreator
    public JobSubmitResponseBody(@JsonProperty(FIELD_NAME_JOB_URL) String jobUrl) {

        this.jobUrl = jobUrl;
    }

    @Override
    public int hashCode() {
        return 73 * jobUrl.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof JobSubmitResponseBody) {
            JobSubmitResponseBody other = (JobSubmitResponseBody) object;
            return Objects.equals(this.jobUrl, other.jobUrl);
        }
        return false;
    }
}

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

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;

/**
 * An actor messages describing details of various jobs. This message is sent for example in
 * response to the {@link RequestJobDetails} message.
 */
public class MultipleJobsDetails implements ResponseBody, Serializable {

    private static final long serialVersionUID = -1526236139616019127L;

    public static final String FIELD_NAME_JOBS = "jobs";

    @JsonProperty(FIELD_NAME_JOBS)
    private final Collection<JobDetails> jobs;

    @JsonCreator
    public MultipleJobsDetails(@JsonProperty(FIELD_NAME_JOBS) Collection<JobDetails> jobs) {
        this.jobs = Preconditions.checkNotNull(jobs);
    }

    // ------------------------------------------------------------------------

    public Collection<JobDetails> getJobs() {
        return jobs;
    }

    @Override
    public String toString() {
        return "MultipleJobsDetails{" + "jobs=" + jobs + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MultipleJobsDetails that = (MultipleJobsDetails) o;
        return Objects.equals(jobs, that.jobs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs);
    }

    // ------------------------------------------------------------------------
}

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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.ApplicationIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.ApplicationIDSerializer;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** Details about an application. */
public class ApplicationDetailsInfo implements ResponseBody, Serializable {

    private static final long serialVersionUID = -3391462110304948764L;

    public static final String FIELD_NAME_APPLICATION_ID = "id";

    public static final String FIELD_NAME_APPLICATION_NAME = "name";

    public static final String FIELD_NAME_APPLICATION_STATUS = "status";

    private static final String FIELD_NAME_START_TIME = "start-time";

    private static final String FIELD_NAME_END_TIME = "end-time";

    private static final String FIELD_NAME_DURATION = "duration";

    public static final String FIELD_NAME_TIMESTAMPS = "timestamps";

    public static final String FIELD_NAME_JOBS = "jobs";

    private final ApplicationID applicationId;

    private final String applicationName;

    private final String applicationStatus;

    private final long startTime;

    private final long endTime;

    private final long duration;

    private final Map<String, Long> timestamps;

    private final Collection<JobDetails> jobs;

    @JsonCreator
    public ApplicationDetailsInfo(
            @JsonDeserialize(using = ApplicationIDDeserializer.class)
                    @JsonProperty(FIELD_NAME_APPLICATION_ID)
                    ApplicationID applicationId,
            @JsonProperty(FIELD_NAME_APPLICATION_NAME) String applicationName,
            @JsonProperty(FIELD_NAME_APPLICATION_STATUS) String applicationStatus,
            @JsonProperty(FIELD_NAME_START_TIME) long startTime,
            @JsonProperty(FIELD_NAME_END_TIME) long endTime,
            @JsonProperty(FIELD_NAME_DURATION) long duration,
            @JsonProperty(FIELD_NAME_TIMESTAMPS) Map<String, Long> timestamps,
            @JsonProperty(FIELD_NAME_JOBS) Collection<JobDetails> jobs) {
        this.applicationId = Preconditions.checkNotNull(applicationId);
        this.applicationName = Preconditions.checkNotNull(applicationName);
        this.applicationStatus = Preconditions.checkNotNull(applicationStatus);
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
        this.timestamps = Preconditions.checkNotNull(timestamps);
        this.jobs = Preconditions.checkNotNull(jobs);
    }

    // ------------------------------------------------------------------------

    @JsonProperty(FIELD_NAME_APPLICATION_ID)
    @JsonSerialize(using = ApplicationIDSerializer.class)
    public ApplicationID getApplicationId() {
        return applicationId;
    }

    @JsonProperty(FIELD_NAME_APPLICATION_NAME)
    public String getApplicationName() {
        return applicationName;
    }

    @JsonProperty(FIELD_NAME_APPLICATION_STATUS)
    public String getApplicationStatus() {
        return applicationStatus;
    }

    @JsonProperty(FIELD_NAME_START_TIME)
    public long getStartTime() {
        return startTime;
    }

    @JsonProperty(FIELD_NAME_END_TIME)
    public long getEndTime() {
        return endTime;
    }

    @JsonProperty(FIELD_NAME_DURATION)
    public long getDuration() {
        return duration;
    }

    @JsonProperty(FIELD_NAME_TIMESTAMPS)
    public Map<String, Long> getTimestamps() {
        return timestamps;
    }

    @JsonProperty(FIELD_NAME_JOBS)
    public Collection<JobDetails> getJobs() {
        return jobs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == ApplicationDetailsInfo.class) {
            ApplicationDetailsInfo that = (ApplicationDetailsInfo) o;

            return this.endTime == that.endTime
                    && this.startTime == that.startTime
                    && this.duration == that.duration
                    && this.applicationStatus.equals(that.applicationStatus)
                    && this.applicationId.equals(that.applicationId)
                    && this.applicationName.equals(that.applicationName)
                    && Objects.equals(this.timestamps, that.timestamps)
                    && Objects.equals(this.jobs, that.jobs);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                applicationId,
                applicationName,
                applicationStatus,
                startTime,
                endTime,
                duration,
                timestamps,
                jobs);
    }

    @Override
    public String toString() {
        return "ApplicationDetailsInfo {"
                + "applicationId="
                + applicationId
                + ", applicationName='"
                + applicationName
                + ", startTime="
                + startTime
                + ", endTime="
                + endTime
                + ", duration="
                + duration
                + ", status="
                + applicationStatus
                + ", timestamps="
                + timestamps
                + ", jobs="
                + jobs
                + '}';
    }

    // ------------------------------------------------------------------------

    public static ApplicationDetailsInfo fromArchivedApplication(
            ArchivedApplication archivedApplication) {
        ApplicationState applicationStatus = archivedApplication.getApplicationStatus();
        long startTime = archivedApplication.getStatusTimestamp(ApplicationState.RUNNING);
        long endTime =
                applicationStatus.isTerminalState()
                        ? archivedApplication.getStatusTimestamp(applicationStatus)
                        : -1L;
        long duration = (endTime >= 0L ? endTime : System.currentTimeMillis()) - startTime;
        final Map<String, Long> timestamps =
                CollectionUtil.newHashMapWithExpectedSize(ApplicationState.values().length);
        for (ApplicationState status : ApplicationState.values()) {
            timestamps.put(status.toString(), archivedApplication.getStatusTimestamp(status));
        }
        return new ApplicationDetailsInfo(
                archivedApplication.getApplicationId(),
                archivedApplication.getApplicationName(),
                applicationStatus.toString(),
                startTime,
                endTime,
                duration,
                timestamps,
                archivedApplication.getJobs());
    }
}

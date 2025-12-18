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
import org.apache.flink.runtime.rest.messages.json.ApplicationIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.ApplicationIDSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Overview of an application. */
public class ApplicationDetails implements Serializable {

    private static final long serialVersionUID = -3391462110304948765L;

    private static final String FIELD_NAME_APPLICATION_ID = "id";
    private static final String FIELD_NAME_APPLICATION_NAME = "name";
    private static final String FIELD_NAME_START_TIME = "start-time";
    private static final String FIELD_NAME_END_TIME = "end-time";
    private static final String FIELD_NAME_DURATION = "duration";
    private static final String FIELD_NAME_STATUS = "status";
    private static final String FIELD_NAME_JOBS = "jobs";

    private final ApplicationID applicationId;

    private final String applicationName;

    private final long startTime;

    private final long endTime;

    private final long duration;

    private final String status;

    private final Map<String, Integer> jobInfo;

    @JsonCreator
    public ApplicationDetails(
            @JsonProperty(FIELD_NAME_APPLICATION_ID)
                    @JsonDeserialize(using = ApplicationIDDeserializer.class)
                    ApplicationID applicationId,
            @JsonProperty(FIELD_NAME_APPLICATION_NAME) String applicationName,
            @JsonProperty(FIELD_NAME_START_TIME) long startTime,
            @JsonProperty(FIELD_NAME_END_TIME) long endTime,
            @JsonProperty(FIELD_NAME_DURATION) long duration,
            @JsonProperty(FIELD_NAME_STATUS) String status,
            @JsonProperty(FIELD_NAME_JOBS) Map<String, Integer> jobInfo) {
        this.applicationId = checkNotNull(applicationId);
        this.applicationName = checkNotNull(applicationName);
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
        this.status = checkNotNull(status);
        this.jobInfo = checkNotNull(jobInfo);
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

    @JsonProperty(FIELD_NAME_STATUS)
    public String getStatus() {
        return status;
    }

    @JsonProperty(FIELD_NAME_JOBS)
    Map<String, Integer> getJobInfo() {
        return jobInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == ApplicationDetails.class) {
            ApplicationDetails that = (ApplicationDetails) o;

            return this.endTime == that.endTime
                    && this.startTime == that.startTime
                    && this.duration == that.duration
                    && this.status.equals(that.status)
                    && this.applicationId.equals(that.applicationId)
                    && this.applicationName.equals(that.applicationName)
                    && this.jobInfo.equals(that.jobInfo);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = applicationId.hashCode();
        result = 31 * result + applicationName.hashCode();
        result = 31 * result + (int) (startTime ^ (startTime >>> 32));
        result = 31 * result + (int) (endTime ^ (endTime >>> 32));
        result = 31 * result + (int) (duration ^ (duration >>> 32));
        result = 31 * result + status.hashCode();
        result = 31 * result + jobInfo.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ApplicationDetails {"
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
                + status
                + ", jobInfo="
                + jobInfo
                + '}';
    }

    public static ApplicationDetails fromArchivedApplication(
            ArchivedApplication archivedApplication) {
        ApplicationState applicationStatus = archivedApplication.getApplicationStatus();
        long startTime = archivedApplication.getStatusTimestamp(ApplicationState.RUNNING);
        long endTime =
                applicationStatus.isTerminalState()
                        ? archivedApplication.getStatusTimestamp(applicationStatus)
                        : -1L;
        long duration = (endTime >= 0L ? endTime : System.currentTimeMillis()) - startTime;
        Map<String, Integer> jobInfo = new HashMap<>();
        archivedApplication.getJobs().stream()
                .map(jobDetails -> jobDetails.getStatus().name())
                .forEach(
                        status ->
                                jobInfo.compute(
                                        status,
                                        (key, oldValue) -> (oldValue == null ? 1 : oldValue + 1)));
        return new ApplicationDetails(
                archivedApplication.getApplicationId(),
                archivedApplication.getApplicationName(),
                startTime,
                endTime,
                duration,
                applicationStatus.toString(),
                jobInfo);
    }
}

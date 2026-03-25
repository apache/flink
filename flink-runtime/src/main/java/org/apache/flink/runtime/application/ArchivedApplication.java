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

package org.apache.flink.runtime.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/** Read-only information about an {@link AbstractApplication}. */
public class ArchivedApplication implements Serializable {

    private static final long serialVersionUID = 7231383912742578429L;

    private final ApplicationID applicationId;

    private final String applicationName;

    private final ApplicationState applicationState;

    private final long[] statusTimestamps;

    private final Map<JobID, ExecutionGraphInfo> jobs;

    private final Collection<ApplicationExceptionHistoryEntry> exceptionHistory;

    public ArchivedApplication(
            ApplicationID applicationId,
            String applicationName,
            ApplicationState applicationState,
            long[] statusTimestamps,
            Map<JobID, ExecutionGraphInfo> jobs,
            Collection<ApplicationExceptionHistoryEntry> exceptionHistory) {
        this.applicationId = applicationId;
        this.applicationName = applicationName;
        this.applicationState = applicationState;
        this.statusTimestamps = statusTimestamps;
        this.jobs = jobs;
        this.exceptionHistory = exceptionHistory;
    }

    public ApplicationID getApplicationId() {
        return applicationId;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public ApplicationState getApplicationStatus() {
        return applicationState;
    }

    public long getStatusTimestamp(ApplicationState status) {
        return this.statusTimestamps[status.ordinal()];
    }

    public Map<JobID, ExecutionGraphInfo> getJobs() {
        return jobs;
    }

    public Collection<ApplicationExceptionHistoryEntry> getExceptionHistory() {
        return exceptionHistory;
    }

    @Override
    public String toString() {
        return "ArchivedApplication{"
                + "applicationId="
                + applicationId
                + ", applicationName='"
                + applicationName
                + '\''
                + ", applicationState="
                + applicationState
                + ", statusTimestamps="
                + Arrays.toString(statusTimestamps)
                + ", jobs="
                + jobs
                + ", exceptionHistory="
                + exceptionHistory
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArchivedApplication that = (ArchivedApplication) o;
        return applicationId.equals(that.applicationId)
                && applicationName.equals(that.applicationName)
                && applicationState == that.applicationState
                && Arrays.equals(statusTimestamps, that.statusTimestamps)
                && jobs.equals(that.jobs)
                && exceptionHistory.equals(that.exceptionHistory);
    }

    @Override
    public int hashCode() {
        return 31 * applicationId.hashCode()
                + 31 * applicationName.hashCode()
                + 31 * applicationState.hashCode()
                + 31 * Arrays.hashCode(statusTimestamps)
                + 31 * jobs.hashCode()
                + 31 * exceptionHistory.hashCode();
    }
}

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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * The result of an application execution. This class collects information about a globally
 * terminated application.
 */
public class ApplicationResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ApplicationID applicationId;
    private final ApplicationState applicationState;
    private final String applicationName;
    private final long startTime;
    private final long endTime;

    private ApplicationResult(
            ApplicationID applicationId,
            ApplicationState applicationState,
            String applicationName,
            long startTime,
            long endTime) {
        this.applicationId = Preconditions.checkNotNull(applicationId);
        this.applicationState = Preconditions.checkNotNull(applicationState);
        this.applicationName = Preconditions.checkNotNull(applicationName);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public ApplicationID getApplicationId() {
        return applicationId;
    }

    public ApplicationState getApplicationState() {
        return applicationState;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    /**
     * Creates the {@link ApplicationResult} from the given {@link ArchivedApplication} which must
     * be in a globally terminal state.
     *
     * @param archivedApplication to create the ApplicationResult from
     * @return ApplicationResult of the given ArchivedApplication
     */
    public static ApplicationResult createFrom(ArchivedApplication archivedApplication) {
        final ApplicationID applicationId = archivedApplication.getApplicationId();
        final ApplicationState applicationState = archivedApplication.getApplicationStatus();

        Preconditions.checkArgument(
                applicationState.isTerminalState(),
                "The application "
                        + archivedApplication.getApplicationName()
                        + '('
                        + applicationId
                        + ") is not in a terminal state. It is in state "
                        + applicationState
                        + '.');

        final ApplicationResult.Builder builder = new ApplicationResult.Builder();
        builder.applicationId(applicationId);
        builder.applicationState(applicationState);
        builder.applicationName(archivedApplication.getApplicationName());

        final long startTime = archivedApplication.getStatusTimestamp(ApplicationState.CREATED);
        final long endTime = archivedApplication.getStatusTimestamp(applicationState);
        builder.startTime(startTime).endTime(endTime);

        return builder.build();
    }

    /** Builder for {@link ApplicationResult}. */
    public static class Builder {

        private ApplicationID applicationId;

        private ApplicationState applicationState;

        private String applicationName = "unknown";

        private long startTime = -1;

        private long endTime = -1;

        public Builder applicationId(final ApplicationID applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        public Builder applicationState(final ApplicationState applicationState) {
            this.applicationState = applicationState;
            return this;
        }

        public Builder applicationName(final String applicationName) {
            this.applicationName = applicationName;
            return this;
        }

        public Builder startTime(final long startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder endTime(final long endTime) {
            this.endTime = endTime;
            return this;
        }

        public ApplicationResult build() {
            return new ApplicationResult(
                    applicationId, applicationState, applicationName, startTime, endTime);
        }
    }
}

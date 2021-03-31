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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * {@code ExceptionHistoryEntry} collects information about a single failure that triggered the
 * scheduler's failure handling.
 */
public class ExceptionHistoryEntry extends ErrorInfo {

    private static final long serialVersionUID = -3855285510064263701L;

    @Nullable private final String failingTaskName;
    @Nullable private final ArchivedTaskManagerLocation taskManagerLocation;

    /**
     * Creates a {@code ExceptionHistoryEntry} representing a global failure from the passed {@code
     * Throwable} and timestamp.
     *
     * @param cause The reason for the failure.
     * @param timestamp The time the failure was caught.
     * @return The {@code ExceptionHistoryEntry} instance.
     */
    public static ExceptionHistoryEntry fromGlobalFailure(Throwable cause, long timestamp) {
        return new ExceptionHistoryEntry(cause, timestamp, null, null);
    }

    /**
     * Creates a {@code ExceptionHistoryEntry} representing a local failure using the passed
     * information.
     *
     * @param execution The {@link AccessExecution} that caused the failure.
     * @param failingTaskName The name of the task the {@code execution} is connected to.
     * @return The {@code ExceptionHistoryEntry} instance.
     */
    public static ExceptionHistoryEntry fromFailedExecution(
            AccessExecution execution, String failingTaskName) {
        ErrorInfo failureInfo =
                execution
                        .getFailureInfo()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "The passed Execution does not provide a failureCause."));
        return new ExceptionHistoryEntry(
                failureInfo.getException(),
                failureInfo.getTimestamp(),
                failingTaskName,
                ArchivedTaskManagerLocation.fromTaskManagerLocation(
                        execution.getAssignedResourceLocation()));
    }

    @VisibleForTesting
    public ExceptionHistoryEntry(
            Throwable cause,
            long timestamp,
            @Nullable String failingTaskName,
            @Nullable ArchivedTaskManagerLocation taskManagerLocation) {
        super(cause, timestamp);
        this.failingTaskName = failingTaskName;
        this.taskManagerLocation = taskManagerLocation;
    }

    public boolean isGlobal() {
        return failingTaskName == null;
    }

    @Nullable
    public String getFailingTaskName() {
        return failingTaskName;
    }

    @Nullable
    public ArchivedTaskManagerLocation getTaskManagerLocation() {
        return taskManagerLocation;
    }

    /**
     * {@code ArchivedTaskManagerLocation} represents a archived (static) version of a {@link
     * TaskManagerLocation}. It overcomes the issue with {@link TaskManagerLocation#inetAddress}
     * being partially transient due to the cache becoming out-dated.
     */
    public static class ArchivedTaskManagerLocation implements Serializable {

        private static final long serialVersionUID = -6596854145482446664L;

        private final ResourceID resourceID;
        private final String addressStr;
        private final int port;
        private final String hostname;
        private final String fqdnHostname;

        /**
         * Creates a {@code ArchivedTaskManagerLocation} copy of the passed {@link
         * TaskManagerLocation}.
         *
         * @param taskManagerLocation The {@code TaskManagerLocation} that's going to be copied.
         * @return The corresponding {@code ArchivedTaskManagerLocation} or {@code null} if {@code
         *     null} was passed.
         */
        @VisibleForTesting
        @Nullable
        public static ArchivedTaskManagerLocation fromTaskManagerLocation(
                TaskManagerLocation taskManagerLocation) {
            if (taskManagerLocation == null) {
                return null;
            }

            return new ArchivedTaskManagerLocation(
                    taskManagerLocation.getResourceID(),
                    taskManagerLocation.addressString(),
                    taskManagerLocation.dataPort(),
                    taskManagerLocation.getHostname(),
                    taskManagerLocation.getFQDNHostname());
        }

        private ArchivedTaskManagerLocation(
                ResourceID resourceID,
                String addressStr,
                int port,
                String hostname,
                String fqdnHost) {
            this.resourceID = resourceID;
            this.addressStr = addressStr;
            this.port = port;
            this.hostname = hostname;
            this.fqdnHostname = fqdnHost;
        }

        public ResourceID getResourceID() {
            return resourceID;
        }

        public String getAddress() {
            return addressStr;
        }

        public int getPort() {
            return port;
        }

        public String getHostname() {
            return hostname;
        }

        public String getFQDNHostname() {
            return fqdnHostname;
        }

        @Override
        public String toString() {
            return new StringJoiner(
                            ", ", ArchivedTaskManagerLocation.class.getSimpleName() + "[", "]")
                    .add("resourceID=" + resourceID)
                    .add("addressStr='" + addressStr + "'")
                    .add("port=" + port)
                    .add("hostname='" + hostname + "'")
                    .add("fqdnHostname='" + fqdnHostname + "'")
                    .toString();
        }
    }
}

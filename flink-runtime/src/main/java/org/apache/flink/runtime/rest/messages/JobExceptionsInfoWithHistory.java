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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@code JobExceptionsInfoWithHistory} extends {@link JobExceptionsInfo} providing a history of
 * previously caused failures. It's the response type of the {@link JobExceptionsHandler}.
 */
public class JobExceptionsInfoWithHistory extends JobExceptionsInfo implements ResponseBody {

    public static final String FIELD_NAME_EXCEPTION_HISTORY = "exceptionHistory";

    @JsonProperty(FIELD_NAME_EXCEPTION_HISTORY)
    private final JobExceptionHistory exceptionHistory;

    @JsonCreator
    public JobExceptionsInfoWithHistory(
            @JsonProperty(FIELD_NAME_ROOT_EXCEPTION) String rootException,
            @JsonProperty(FIELD_NAME_TIMESTAMP) Long rootTimestamp,
            @JsonProperty(FIELD_NAME_ALL_EXCEPTIONS) List<ExecutionExceptionInfo> allExceptions,
            @JsonProperty(FIELD_NAME_TRUNCATED) boolean truncated,
            @JsonProperty(FIELD_NAME_EXCEPTION_HISTORY) JobExceptionHistory exceptionHistory) {
        super(rootException, rootTimestamp, allExceptions, truncated);
        this.exceptionHistory = exceptionHistory;
    }

    public JobExceptionsInfoWithHistory(JobExceptionHistory exceptionHistory) {
        this(null, null, Collections.emptyList(), false, exceptionHistory);
    }

    @JsonIgnore
    public JobExceptionHistory getExceptionHistory() {
        return exceptionHistory;
    }

    // hashCode and equals are necessary for the test classes deriving from
    // RestResponseMarshallingTestBase
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobExceptionsInfoWithHistory that = (JobExceptionsInfoWithHistory) o;
        return this.isTruncated() == that.isTruncated()
                && Objects.equals(this.getRootException(), that.getRootException())
                && Objects.equals(this.getRootTimestamp(), that.getRootTimestamp())
                && Objects.equals(this.getAllExceptions(), that.getAllExceptions())
                && Objects.equals(exceptionHistory, that.exceptionHistory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                isTruncated(),
                getRootException(),
                getRootTimestamp(),
                getAllExceptions(),
                exceptionHistory);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", JobExceptionsInfoWithHistory.class.getSimpleName() + "[", "]")
                .add("rootException='" + getRootException() + "'")
                .add("rootTimestamp=" + getRootTimestamp())
                .add("allExceptions=" + getAllExceptions())
                .add("truncated=" + isTruncated())
                .add("exceptionHistory=" + exceptionHistory)
                .toString();
    }

    /** {@code JobExceptionHistory} collects all previously caught errors. */
    public static final class JobExceptionHistory {

        public static final String FIELD_NAME_ENTRIES = "entries";
        public static final String FIELD_NAME_TRUNCATED = "truncated";

        @JsonProperty(FIELD_NAME_ENTRIES)
        private final List<RootExceptionInfo> entries;

        @JsonProperty(FIELD_NAME_TRUNCATED)
        private final boolean truncated;

        @JsonCreator
        public JobExceptionHistory(
                @JsonProperty(FIELD_NAME_ENTRIES) List<RootExceptionInfo> entries,
                @JsonProperty(FIELD_NAME_TRUNCATED) boolean truncated) {
            this.entries = entries;
            this.truncated = truncated;
        }

        @JsonIgnore
        public List<RootExceptionInfo> getEntries() {
            return entries;
        }

        @JsonIgnore
        public boolean isTruncated() {
            return truncated;
        }

        // hashCode and equals are necessary for the test classes deriving from
        // RestResponseMarshallingTestBase
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JobExceptionHistory that = (JobExceptionHistory) o;
            return this.isTruncated() == that.isTruncated()
                    && Objects.equals(entries, that.entries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entries, truncated);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", JobExceptionHistory.class.getSimpleName() + "[", "]")
                    .add("entries=" + entries)
                    .add("truncated=" + truncated)
                    .toString();
        }
    }

    /**
     * Json equivalent of {@link
     * org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry}.
     */
    public static class ExceptionInfo {

        public static final String FIELD_NAME_EXCEPTION_NAME = "exceptionName";
        public static final String FIELD_NAME_EXCEPTION_STACKTRACE = "stacktrace";
        public static final String FIELD_NAME_EXCEPTION_TIMESTAMP = "timestamp";
        public static final String FIELD_NAME_TASK_NAME = "taskName";
        public static final String FIELD_NAME_LOCATION = "location";
        public static final String FIELD_NAME_TASK_MANAGER_ID = "taskManagerId";
        public static final String FIELD_NAME_FAILURE_LABELS = "failureLabels";

        @JsonProperty(FIELD_NAME_EXCEPTION_NAME)
        private final String exceptionName;

        @JsonProperty(FIELD_NAME_EXCEPTION_STACKTRACE)
        private final String stacktrace;

        @JsonProperty(FIELD_NAME_EXCEPTION_TIMESTAMP)
        private final long timestamp;

        @JsonInclude(NON_NULL)
        @JsonProperty(FIELD_NAME_TASK_NAME)
        @Nullable
        private final String taskName;

        @JsonInclude(NON_NULL)
        @JsonProperty(FIELD_NAME_LOCATION)
        @Nullable
        private final String location;

        @JsonInclude(NON_NULL)
        @JsonProperty(FIELD_NAME_TASK_MANAGER_ID)
        @Nullable
        private final String taskManagerId;

        @JsonProperty(FIELD_NAME_FAILURE_LABELS)
        private final Map<String, String> failureLabels;

        public ExceptionInfo(String exceptionName, String stacktrace, long timestamp) {
            this(exceptionName, stacktrace, timestamp, Collections.emptyMap(), null, null, null);
        }

        @JsonCreator
        public ExceptionInfo(
                @JsonProperty(FIELD_NAME_EXCEPTION_NAME) String exceptionName,
                @JsonProperty(FIELD_NAME_EXCEPTION_STACKTRACE) String stacktrace,
                @JsonProperty(FIELD_NAME_EXCEPTION_TIMESTAMP) long timestamp,
                @JsonProperty(FIELD_NAME_FAILURE_LABELS) Map<String, String> failureLabels,
                @JsonProperty(FIELD_NAME_TASK_NAME) @Nullable String taskName,
                @JsonProperty(FIELD_NAME_LOCATION) @Nullable String location,
                @JsonProperty(FIELD_NAME_TASK_MANAGER_ID) @Nullable String taskManagerId) {
            this.exceptionName = checkNotNull(exceptionName);
            this.stacktrace = checkNotNull(stacktrace);
            this.timestamp = timestamp;
            this.failureLabels = checkNotNull(failureLabels);
            this.taskName = taskName;
            this.location = location;
            this.taskManagerId = taskManagerId;
        }

        @JsonIgnore
        public String getExceptionName() {
            return exceptionName;
        }

        @JsonIgnore
        public String getStacktrace() {
            return stacktrace;
        }

        @JsonIgnore
        public long getTimestamp() {
            return timestamp;
        }

        @JsonIgnore
        @Nullable
        public String getTaskName() {
            return taskName;
        }

        @JsonIgnore
        @Nullable
        public String getLocation() {
            return location;
        }

        @JsonIgnore
        @Nullable
        public String getTaskManagerId() {
            return taskManagerId;
        }

        @JsonIgnore
        public Map<String, String> getFailureLabels() {
            return failureLabels;
        }

        // hashCode and equals are necessary for the test classes deriving from
        // RestResponseMarshallingTestBase
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExceptionInfo that = (ExceptionInfo) o;
            return exceptionName.equals(that.exceptionName)
                    && stacktrace.equals(that.stacktrace)
                    && Objects.equals(timestamp, that.timestamp)
                    && Objects.equals(failureLabels, that.failureLabels)
                    && Objects.equals(taskName, that.taskName)
                    && Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    exceptionName, stacktrace, timestamp, failureLabels, taskName, location);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", ExceptionInfo.class.getSimpleName() + "[", "]")
                    .add("exceptionName='" + exceptionName + "'")
                    .add("stacktrace='" + stacktrace + "'")
                    .add("timestamp=" + timestamp)
                    .add("failureLabels=" + failureLabels)
                    .add("taskName='" + taskName + "'")
                    .add("location='" + location + "'")
                    .toString();
        }
    }

    /**
     * Json equivalent of {@link
     * org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry}.
     */
    public static class RootExceptionInfo extends ExceptionInfo {

        public static final String FIELD_NAME_CONCURRENT_EXCEPTIONS = "concurrentExceptions";

        @JsonProperty(FIELD_NAME_CONCURRENT_EXCEPTIONS)
        private final Collection<ExceptionInfo> concurrentExceptions;

        public RootExceptionInfo(
                String exceptionName,
                String stacktrace,
                long timestamp,
                Map<String, String> failureLabels,
                Collection<ExceptionInfo> concurrentExceptions) {
            this(
                    exceptionName,
                    stacktrace,
                    timestamp,
                    failureLabels,
                    null,
                    null,
                    null,
                    concurrentExceptions);
        }

        @JsonCreator
        public RootExceptionInfo(
                @JsonProperty(FIELD_NAME_EXCEPTION_NAME) String exceptionName,
                @JsonProperty(FIELD_NAME_EXCEPTION_STACKTRACE) String stacktrace,
                @JsonProperty(FIELD_NAME_EXCEPTION_TIMESTAMP) long timestamp,
                @JsonProperty(FIELD_NAME_FAILURE_LABELS) Map<String, String> failureLabels,
                @JsonProperty(FIELD_NAME_TASK_NAME) @Nullable String taskName,
                @JsonProperty(FIELD_NAME_LOCATION) @Nullable String location,
                @JsonProperty(FIELD_NAME_TASK_MANAGER_ID) @Nullable String taskManagerId,
                @JsonProperty(FIELD_NAME_CONCURRENT_EXCEPTIONS)
                        Collection<ExceptionInfo> concurrentExceptions) {
            super(
                    exceptionName,
                    stacktrace,
                    timestamp,
                    failureLabels,
                    taskName,
                    location,
                    taskManagerId);
            this.concurrentExceptions = concurrentExceptions;
        }

        @JsonIgnore
        public Collection<ExceptionInfo> getConcurrentExceptions() {
            return concurrentExceptions;
        }

        // hashCode and equals are necessary for the test classes deriving from
        // RestResponseMarshallingTestBase
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass() || !super.equals(o)) {
                return false;
            }
            RootExceptionInfo that = (RootExceptionInfo) o;
            return getConcurrentExceptions().equals(that.getConcurrentExceptions());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), getConcurrentExceptions());
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", RootExceptionInfo.class.getSimpleName() + "[", "]")
                    .add("exceptionName='" + getExceptionName() + "'")
                    .add("stacktrace='" + getStacktrace() + "'")
                    .add("timestamp=" + getTimestamp())
                    .add("taskName='" + getTaskName() + "'")
                    .add("location='" + getLocation() + "'")
                    .add("concurrentExceptions=" + getConcurrentExceptions())
                    .toString();
        }
    }
}

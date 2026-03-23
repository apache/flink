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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.application.ApplicationExceptionHistoryEntry;
import org.apache.flink.runtime.rest.handler.application.ApplicationExceptionsHandler;
import org.apache.flink.runtime.rest.messages.json.JobIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobIDSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@code ApplicationExceptionsInfoWithHistory} providing a history of previously caused failures.
 * It's the response type of the {@link ApplicationExceptionsHandler}.
 */
public class ApplicationExceptionsInfoWithHistory implements ResponseBody {

    public static final String FIELD_NAME_EXCEPTION_HISTORY = "exceptionHistory";

    @JsonProperty(FIELD_NAME_EXCEPTION_HISTORY)
    private final ApplicationExceptionHistory exceptionHistory;

    @JsonCreator
    public ApplicationExceptionsInfoWithHistory(
            @JsonProperty(FIELD_NAME_EXCEPTION_HISTORY)
                    ApplicationExceptionHistory exceptionHistory) {
        this.exceptionHistory = exceptionHistory;
    }

    @JsonIgnore
    public ApplicationExceptionHistory getExceptionHistory() {
        return exceptionHistory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ApplicationExceptionsInfoWithHistory that = (ApplicationExceptionsInfoWithHistory) o;
        return Objects.equals(exceptionHistory, that.exceptionHistory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exceptionHistory);
    }

    @Override
    public String toString() {
        return new StringJoiner(
                        ", ", ApplicationExceptionsInfoWithHistory.class.getSimpleName() + "[", "]")
                .add("exceptionHistory=" + exceptionHistory)
                .toString();
    }

    public static ApplicationExceptionsInfoWithHistory fromApplicationExceptionHistory(
            Collection<ApplicationExceptionHistoryEntry> exceptions) {
        return new ApplicationExceptionsInfoWithHistory(
                new ApplicationExceptionHistory(
                        exceptions.stream()
                                .map(
                                        exception ->
                                                new ApplicationExceptionInfo(
                                                        exception
                                                                .getException()
                                                                .getOriginalErrorClassName(),
                                                        exception.getExceptionAsString(),
                                                        exception.getTimestamp(),
                                                        exception.getJobId().orElse(null)))
                                .collect(Collectors.toList())));
    }

    public static final class ApplicationExceptionHistory {

        public static final String FIELD_NAME_ENTRIES = "entries";

        @JsonProperty(FIELD_NAME_ENTRIES)
        private final List<ApplicationExceptionInfo> entries;

        @JsonCreator
        public ApplicationExceptionHistory(
                @JsonProperty(FIELD_NAME_ENTRIES) List<ApplicationExceptionInfo> entries) {
            this.entries = entries;
        }

        @JsonIgnore
        public List<ApplicationExceptionInfo> getEntries() {
            return entries;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ApplicationExceptionHistory that = (ApplicationExceptionHistory) o;
            return Objects.equals(entries, that.entries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entries);
        }

        @Override
        public String toString() {
            return new StringJoiner(
                            ", ", ApplicationExceptionHistory.class.getSimpleName() + "[", "]")
                    .add("entries=" + entries)
                    .toString();
        }
    }

    public static class ApplicationExceptionInfo {

        public static final String FIELD_NAME_EXCEPTION_NAME = "exceptionName";
        public static final String FIELD_NAME_EXCEPTION_STACKTRACE = "stacktrace";
        public static final String FIELD_NAME_EXCEPTION_TIMESTAMP = "timestamp";
        public static final String FIELD_NAME_JOB_ID = "jobId";

        @JsonProperty(FIELD_NAME_EXCEPTION_NAME)
        private final String exceptionName;

        @JsonProperty(FIELD_NAME_EXCEPTION_STACKTRACE)
        private final String stacktrace;

        @JsonProperty(FIELD_NAME_EXCEPTION_TIMESTAMP)
        private final long timestamp;

        @JsonInclude(NON_NULL)
        @JsonProperty(FIELD_NAME_JOB_ID)
        @JsonSerialize(using = JobIDSerializer.class)
        @Nullable
        private final JobID jobId;

        public ApplicationExceptionInfo(String exceptionName, String stacktrace, long timestamp) {
            this(exceptionName, stacktrace, timestamp, null);
        }

        @JsonCreator
        public ApplicationExceptionInfo(
                @JsonProperty(FIELD_NAME_EXCEPTION_NAME) String exceptionName,
                @JsonProperty(FIELD_NAME_EXCEPTION_STACKTRACE) String stacktrace,
                @JsonProperty(FIELD_NAME_EXCEPTION_TIMESTAMP) long timestamp,
                @JsonProperty(FIELD_NAME_JOB_ID)
                        @Nullable
                        @JsonDeserialize(using = JobIDDeserializer.class)
                        JobID jobId) {
            this.exceptionName = checkNotNull(exceptionName);
            this.stacktrace = checkNotNull(stacktrace);
            this.timestamp = timestamp;
            this.jobId = jobId;
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
        public JobID getJobId() {
            return jobId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ApplicationExceptionInfo that = (ApplicationExceptionInfo) o;
            return exceptionName.equals(that.exceptionName)
                    && stacktrace.equals(that.stacktrace)
                    && Objects.equals(timestamp, that.timestamp)
                    && Objects.equals(jobId, that.jobId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exceptionName, stacktrace, timestamp, jobId);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", ApplicationExceptionInfo.class.getSimpleName() + "[", "]")
                    .add("exceptionName='" + exceptionName + "'")
                    .add("stacktrace='" + stacktrace + "'")
                    .add("timestamp=" + timestamp)
                    .add("jobId=" + jobId)
                    .toString();
        }
    }
}

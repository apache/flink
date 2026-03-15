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

package org.apache.flink.runtime.rest.handler.legacy.messages;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Response body for Diagnosis Advisor containing diagnostic suggestions. */
public class DiagnosisResponseBody {

    private static final String FIELD_NAME_DIAGNOSTICS = "diagnostics";
    private static final String FIELD_NAME_TIMESTAMP = "timestamp";

    @JsonProperty(FIELD_NAME_DIAGNOSTICS)
    private final List<DiagnosticSuggestion> diagnostics;

    @JsonProperty(FIELD_NAME_TIMESTAMP)
    private final String timestamp;

    @JsonCreator
    public DiagnosisResponseBody(
            @JsonProperty(FIELD_NAME_DIAGNOSTICS) List<DiagnosticSuggestion> diagnostics,
            @JsonProperty(FIELD_NAME_TIMESTAMP) String timestamp) {
        this.diagnostics = diagnostics;
        this.timestamp = timestamp;
    }

    @JsonProperty(FIELD_NAME_DIAGNOSTICS)
    public List<DiagnosticSuggestion> getDiagnostics() {
        return diagnostics;
    }

    @JsonProperty(FIELD_NAME_TIMESTAMP)
    public String getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DiagnosisResponseBody that = (DiagnosisResponseBody) o;
        return Objects.equals(diagnostics, that.diagnostics)
                && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(diagnostics, timestamp);
    }

    /** Represents a single diagnostic suggestion with severity and recommended actions. */
    public static class DiagnosticSuggestion {
        private static final String FIELD_NAME_SEVERITY = "severity";
        private static final String FIELD_NAME_TITLE = "title";
        private static final String FIELD_NAME_MESSAGE = "message";
        private static final String FIELD_NAME_METRICS = "metrics";
        private static final String FIELD_NAME_ACTIONS = "actions";

        @JsonProperty(FIELD_NAME_SEVERITY)
        private final String severity;

        @JsonProperty(FIELD_NAME_TITLE)
        private final String title;

        @JsonProperty(FIELD_NAME_MESSAGE)
        private final String message;

        @JsonProperty(FIELD_NAME_METRICS)
        private final Map<String, Object> metrics;

        @JsonProperty(FIELD_NAME_ACTIONS)
        private final List<String> actions;

        @JsonCreator
        public DiagnosticSuggestion(
                @JsonProperty(FIELD_NAME_SEVERITY) String severity,
                @JsonProperty(FIELD_NAME_TITLE) String title,
                @JsonProperty(FIELD_NAME_MESSAGE) String message,
                @JsonProperty(FIELD_NAME_METRICS) Map<String, Object> metrics,
                @JsonProperty(FIELD_NAME_ACTIONS) List<String> actions) {
            this.severity = severity;
            this.title = title;
            this.message = message;
            this.metrics = metrics;
            this.actions = actions;
        }

        @JsonProperty(FIELD_NAME_SEVERITY)
        public String getSeverity() {
            return severity;
        }

        @JsonProperty(FIELD_NAME_TITLE)
        public String getTitle() {
            return title;
        }

        @JsonProperty(FIELD_NAME_MESSAGE)
        public String getMessage() {
            return message;
        }

        @JsonProperty(FIELD_NAME_METRICS)
        public Map<String, Object> getMetrics() {
            return metrics;
        }

        @JsonProperty(FIELD_NAME_ACTIONS)
        public List<String> getActions() {
            return actions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DiagnosticSuggestion that = (DiagnosticSuggestion) o;
            return Objects.equals(severity, that.severity)
                    && Objects.equals(title, that.title)
                    && Objects.equals(message, that.message)
                    && Objects.equals(metrics, that.metrics)
                    && Objects.equals(actions, that.actions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(severity, title, message, metrics, actions);
        }
    }
}

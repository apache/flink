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

package org.apache.flink.runtime.rest.messages.job.rescales;

import org.apache.flink.runtime.rest.handler.job.rescales.JobRescalesOverviewHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesSummarySnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TerminalState;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** Response body for {@link JobRescalesOverviewHandler}. */
@Schema(name = "JobRescalesOverview")
public class JobRescalesOverview implements ResponseBody, Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_RESCALES_COUNTS = "rescalesCounts";
    public static final String FIELD_NAME_LATEST = "latest";

    @JsonProperty(FIELD_NAME_RESCALES_COUNTS)
    private final RescalesCounts rescalesCounts;

    @JsonProperty(FIELD_NAME_LATEST)
    private final LatestRescales latest;

    @JsonCreator
    public JobRescalesOverview(
            @JsonProperty(FIELD_NAME_RESCALES_COUNTS) RescalesCounts rescalesCounts,
            @JsonProperty(FIELD_NAME_LATEST) LatestRescales latest) {
        this.rescalesCounts = rescalesCounts;
        this.latest = latest;
    }

    public static JobRescalesOverview fromRescalesStatsSnapshot(RescalesStatsSnapshot snapshot) {
        RescalesSummarySnapshot summarySnapshot = snapshot.getRescalesSummarySnapshot();
        RescalesCounts counts =
                new RescalesCounts(
                        summarySnapshot.getIgnoredRescalesCount(),
                        summarySnapshot.getInProgressRescaleCount(),
                        summarySnapshot.getCompletedRescalesCount(),
                        summarySnapshot.getFailedRescalesCount());
        Rescale latestCompletedRescale = snapshot.getLatestRescale(TerminalState.COMPLETED);
        JobRescaleDetails latestCompleted =
                latestCompletedRescale == null
                        ? null
                        : JobRescaleDetails.fromRescale(latestCompletedRescale, false);

        Rescale latestFailedRescale = snapshot.getLatestRescale(TerminalState.FAILED);
        JobRescaleDetails latestFailed =
                latestFailedRescale == null
                        ? null
                        : JobRescaleDetails.fromRescale(latestFailedRescale, false);

        Rescale latestIgnoredRescale = snapshot.getLatestRescale(TerminalState.IGNORED);
        JobRescaleDetails latestIgnored =
                latestIgnoredRescale == null
                        ? null
                        : JobRescaleDetails.fromRescale(latestIgnoredRescale, false);
        LatestRescales latest = new LatestRescales(latestCompleted, latestFailed, latestIgnored);
        return new JobRescalesOverview(counts, latest);
    }

    public RescalesCounts getRescalesCounts() {
        return rescalesCounts;
    }

    public LatestRescales getLatest() {
        return latest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobRescalesOverview that = (JobRescalesOverview) o;
        return Objects.equals(rescalesCounts, that.rescalesCounts)
                && Objects.equals(latest, that.latest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rescalesCounts, latest);
    }

    /** Counts for rescales. */
    public static class RescalesCounts implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final String FIELD_NAME_IGNORED = "ignored";
        public static final String FIELD_NAME_IN_PROGRESS = "inProgress";
        public static final String FIELD_NAME_COMPLETED = "completed";
        public static final String FIELD_NAME_FAILED = "failed";

        @JsonProperty(FIELD_NAME_IGNORED)
        private final long ignored;

        @JsonProperty(FIELD_NAME_IN_PROGRESS)
        private final long inProgress;

        @JsonProperty(FIELD_NAME_COMPLETED)
        private final long completed;

        @JsonProperty(FIELD_NAME_FAILED)
        private final long failed;

        @JsonCreator
        public RescalesCounts(
                @JsonProperty(FIELD_NAME_IGNORED) long ignored,
                @JsonProperty(FIELD_NAME_IN_PROGRESS) long inProgress,
                @JsonProperty(FIELD_NAME_COMPLETED) long completed,
                @JsonProperty(FIELD_NAME_FAILED) long failed) {
            this.ignored = ignored;
            this.inProgress = inProgress;
            this.completed = completed;
            this.failed = failed;
        }

        public long getIgnored() {
            return ignored;
        }

        public long getInProgress() {
            return inProgress;
        }

        public long getCompleted() {
            return completed;
        }

        public long getFailed() {
            return failed;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RescalesCounts that = (RescalesCounts) o;
            return ignored == that.ignored
                    && inProgress == that.inProgress
                    && completed == that.completed
                    && failed == that.failed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ignored, inProgress, completed, failed);
        }
    }

    /** Latest rescales by terminal state. */
    public static class LatestRescales implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final String FIELD_NAME_COMPLETED = "completed";
        public static final String FIELD_NAME_FAILED = "failed";
        public static final String FIELD_NAME_IGNORED = "ignored";

        @Nullable
        @JsonProperty(FIELD_NAME_COMPLETED)
        private final JobRescaleDetails completed;

        @Nullable
        @JsonProperty(FIELD_NAME_FAILED)
        private final JobRescaleDetails failed;

        @Nullable
        @JsonProperty(FIELD_NAME_IGNORED)
        private final JobRescaleDetails ignored;

        @JsonCreator
        public LatestRescales(
                @JsonProperty(FIELD_NAME_COMPLETED) @Nullable JobRescaleDetails completed,
                @JsonProperty(FIELD_NAME_FAILED) @Nullable JobRescaleDetails failed,
                @JsonProperty(FIELD_NAME_IGNORED) @Nullable JobRescaleDetails ignored) {
            this.completed = completed;
            this.failed = failed;
            this.ignored = ignored;
        }

        @Nullable
        public JobRescaleDetails getCompleted() {
            return completed;
        }

        @Nullable
        public JobRescaleDetails getFailed() {
            return failed;
        }

        @Nullable
        public JobRescaleDetails getIgnored() {
            return ignored;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LatestRescales that = (LatestRescales) o;
            return Objects.equals(completed, that.completed)
                    && Objects.equals(failed, that.failed)
                    && Objects.equals(ignored, that.ignored);
        }

        @Override
        public int hashCode() {
            return Objects.hash(completed, failed, ignored);
        }
    }
}

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

import org.apache.flink.runtime.rest.handler.job.rescales.JobRescalesSummaryHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.util.stats.StatsSummaryDto;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesSummarySnapshot;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;
import java.util.Objects;

/** Response body for {@link JobRescalesSummaryHandler}. */
@Schema(name = "JobRescalesSummary")
public class JobRescalesSummary implements ResponseBody, Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_RESCALES_COUNTS = "rescalesCounts";
    public static final String FIELD_NAME_RESCALES_DURATION_STATS = "rescalesDurationStatsInMillis";
    public static final String FIELD_NAME_COMPLETED_RESCALES_DURATION_STATS =
            "completedRescalesDurationStatsInMillis";
    public static final String FIELD_NAME_IGNORED_RESCALES_DURATION_STATS =
            "ignoredRescalesDurationStatsInMillis";
    public static final String FIELD_NAME_FAILED_RESCALES_DURATION_STATS =
            "failedRescalesDurationStatsInMillis";

    @JsonProperty(FIELD_NAME_RESCALES_COUNTS)
    private final JobRescalesOverview.RescalesCounts rescalesCounts;

    @JsonProperty(FIELD_NAME_RESCALES_DURATION_STATS)
    private final StatsSummaryDto rescalesDurationStatsInMillis;

    @JsonProperty(FIELD_NAME_COMPLETED_RESCALES_DURATION_STATS)
    private final StatsSummaryDto completedRescalesDurationStatsInMillis;

    @JsonProperty(FIELD_NAME_IGNORED_RESCALES_DURATION_STATS)
    private final StatsSummaryDto ignoredRescalesDurationStatsInMillis;

    @JsonProperty(FIELD_NAME_FAILED_RESCALES_DURATION_STATS)
    private final StatsSummaryDto failedRescalesDurationStatsInMillis;

    @JsonCreator
    public JobRescalesSummary(
            @JsonProperty(FIELD_NAME_RESCALES_COUNTS)
                    JobRescalesOverview.RescalesCounts rescalesCounts,
            @JsonProperty(FIELD_NAME_RESCALES_DURATION_STATS)
                    StatsSummaryDto rescalesDurationStatsInMillis,
            @JsonProperty(FIELD_NAME_COMPLETED_RESCALES_DURATION_STATS)
                    StatsSummaryDto completedRescalesDurationStatsInMillis,
            @JsonProperty(FIELD_NAME_IGNORED_RESCALES_DURATION_STATS)
                    StatsSummaryDto ignoredRescalesDurationStatsInMillis,
            @JsonProperty(FIELD_NAME_FAILED_RESCALES_DURATION_STATS)
                    StatsSummaryDto failedRescalesDurationStatsInMillis) {
        this.rescalesCounts = rescalesCounts;
        this.rescalesDurationStatsInMillis = rescalesDurationStatsInMillis;
        this.completedRescalesDurationStatsInMillis = completedRescalesDurationStatsInMillis;
        this.ignoredRescalesDurationStatsInMillis = ignoredRescalesDurationStatsInMillis;
        this.failedRescalesDurationStatsInMillis = failedRescalesDurationStatsInMillis;
    }

    public static JobRescalesSummary fromRescalesStatsSnapshot(RescalesStatsSnapshot snapshot) {
        RescalesSummarySnapshot summarySnapshot = snapshot.getRescalesSummarySnapshot();
        JobRescalesOverview.RescalesCounts counts =
                new JobRescalesOverview.RescalesCounts(
                        summarySnapshot.getIgnoredRescalesCount(),
                        summarySnapshot.getInProgressRescaleCount(),
                        summarySnapshot.getCompletedRescalesCount(),
                        summarySnapshot.getFailedRescalesCount());
        return new JobRescalesSummary(
                counts,
                StatsSummaryDto.valueOf(summarySnapshot.getAllTerminatedSummarySnapshot()),
                StatsSummaryDto.valueOf(summarySnapshot.getCompletedRescalesSummarySnapshot()),
                StatsSummaryDto.valueOf(summarySnapshot.getIgnoredRescalesSummarySnapshot()),
                StatsSummaryDto.valueOf(summarySnapshot.getFailedRescalesSummarySnapshot()));
    }

    public JobRescalesOverview.RescalesCounts getRescalesCounts() {
        return rescalesCounts;
    }

    public StatsSummaryDto getRescalesDurationStatsInMillis() {
        return rescalesDurationStatsInMillis;
    }

    public StatsSummaryDto getCompletedRescalesDurationStatsInMillis() {
        return completedRescalesDurationStatsInMillis;
    }

    public StatsSummaryDto getIgnoredRescalesDurationStatsInMillis() {
        return ignoredRescalesDurationStatsInMillis;
    }

    public StatsSummaryDto getFailedRescalesDurationStatsInMillis() {
        return failedRescalesDurationStatsInMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobRescalesSummary that = (JobRescalesSummary) o;
        return Objects.equals(rescalesCounts, that.rescalesCounts)
                && Objects.equals(rescalesDurationStatsInMillis, that.rescalesDurationStatsInMillis)
                && Objects.equals(
                        completedRescalesDurationStatsInMillis,
                        that.completedRescalesDurationStatsInMillis)
                && Objects.equals(
                        ignoredRescalesDurationStatsInMillis,
                        that.ignoredRescalesDurationStatsInMillis)
                && Objects.equals(
                        failedRescalesDurationStatsInMillis,
                        that.failedRescalesDurationStatsInMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rescalesCounts,
                rescalesDurationStatsInMillis,
                completedRescalesDurationStatsInMillis,
                ignoredRescalesDurationStatsInMillis,
                failedRescalesDurationStatsInMillis);
    }
}

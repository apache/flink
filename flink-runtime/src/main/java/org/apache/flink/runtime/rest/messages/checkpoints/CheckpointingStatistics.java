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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointingStatisticsHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Response of the {@link CheckpointingStatisticsHandler}. This class contains information about the
 * checkpointing of a given job.
 */
public class CheckpointingStatistics implements ResponseBody {

    public static final String FIELD_NAME_COUNTS = "counts";

    public static final String FIELD_NAME_SUMMARY = "summary";

    public static final String FIELD_NAME_LATEST_CHECKPOINTS = "latest";

    public static final String FIELD_NAME_HISTORY = "history";

    @JsonProperty(FIELD_NAME_COUNTS)
    private final Counts counts;

    @JsonProperty(FIELD_NAME_SUMMARY)
    private final Summary summary;

    @JsonProperty(FIELD_NAME_LATEST_CHECKPOINTS)
    private final LatestCheckpoints latestCheckpoints;

    @JsonProperty(FIELD_NAME_HISTORY)
    private final List<CheckpointStatistics> history;

    @JsonCreator
    public CheckpointingStatistics(
            @JsonProperty(FIELD_NAME_COUNTS) Counts counts,
            @JsonProperty(FIELD_NAME_SUMMARY) Summary summary,
            @JsonProperty(FIELD_NAME_LATEST_CHECKPOINTS) LatestCheckpoints latestCheckpoints,
            @JsonProperty(FIELD_NAME_HISTORY) List<CheckpointStatistics> history) {
        this.counts = Preconditions.checkNotNull(counts);
        this.summary = Preconditions.checkNotNull(summary);
        this.latestCheckpoints = Preconditions.checkNotNull(latestCheckpoints);
        this.history = Preconditions.checkNotNull(history);
    }

    public Counts getCounts() {
        return counts;
    }

    public Summary getSummary() {
        return summary;
    }

    public LatestCheckpoints getLatestCheckpoints() {
        return latestCheckpoints;
    }

    public List<CheckpointStatistics> getHistory() {
        return history;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointingStatistics that = (CheckpointingStatistics) o;
        return Objects.equals(counts, that.counts)
                && Objects.equals(summary, that.summary)
                && Objects.equals(latestCheckpoints, that.latestCheckpoints)
                && Objects.equals(history, that.history);
    }

    @Override
    public int hashCode() {
        return Objects.hash(counts, summary, latestCheckpoints, history);
    }

    // ------------------------------------------------------------------
    // Inner classes
    // ------------------------------------------------------------------

    /** Checkpoint counts. */
    public static final class Counts {

        public static final String FIELD_NAME_RESTORED_CHECKPOINTS = "restored";

        public static final String FIELD_NAME_TOTAL_CHECKPOINTS = "total";

        public static final String FIELD_NAME_IN_PROGRESS_CHECKPOINTS = "in_progress";

        public static final String FIELD_NAME_COMPLETED_CHECKPOINTS = "completed";

        public static final String FIELD_NAME_FAILED_CHECKPOINTS = "failed";

        @JsonProperty(FIELD_NAME_RESTORED_CHECKPOINTS)
        private final long numberRestoredCheckpoints;

        @JsonProperty(FIELD_NAME_TOTAL_CHECKPOINTS)
        private final long totalNumberCheckpoints;

        @JsonProperty(FIELD_NAME_IN_PROGRESS_CHECKPOINTS)
        private final int numberInProgressCheckpoints;

        @JsonProperty(FIELD_NAME_COMPLETED_CHECKPOINTS)
        private final long numberCompletedCheckpoints;

        @JsonProperty(FIELD_NAME_FAILED_CHECKPOINTS)
        private final long numberFailedCheckpoints;

        @JsonCreator
        public Counts(
                @JsonProperty(FIELD_NAME_RESTORED_CHECKPOINTS) long numberRestoredCheckpoints,
                @JsonProperty(FIELD_NAME_TOTAL_CHECKPOINTS) long totalNumberCheckpoints,
                @JsonProperty(FIELD_NAME_IN_PROGRESS_CHECKPOINTS) int numberInProgressCheckpoints,
                @JsonProperty(FIELD_NAME_COMPLETED_CHECKPOINTS) long numberCompletedCheckpoints,
                @JsonProperty(FIELD_NAME_FAILED_CHECKPOINTS) long numberFailedCheckpoints) {
            this.numberRestoredCheckpoints = numberRestoredCheckpoints;
            this.totalNumberCheckpoints = totalNumberCheckpoints;
            this.numberInProgressCheckpoints = numberInProgressCheckpoints;
            this.numberCompletedCheckpoints = numberCompletedCheckpoints;
            this.numberFailedCheckpoints = numberFailedCheckpoints;
        }

        public long getNumberRestoredCheckpoints() {
            return numberRestoredCheckpoints;
        }

        public long getTotalNumberCheckpoints() {
            return totalNumberCheckpoints;
        }

        public int getNumberInProgressCheckpoints() {
            return numberInProgressCheckpoints;
        }

        public long getNumberCompletedCheckpoints() {
            return numberCompletedCheckpoints;
        }

        public long getNumberFailedCheckpoints() {
            return numberFailedCheckpoints;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Counts counts = (Counts) o;
            return numberRestoredCheckpoints == counts.numberRestoredCheckpoints
                    && totalNumberCheckpoints == counts.totalNumberCheckpoints
                    && numberInProgressCheckpoints == counts.numberInProgressCheckpoints
                    && numberCompletedCheckpoints == counts.numberCompletedCheckpoints
                    && numberFailedCheckpoints == counts.numberFailedCheckpoints;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    numberRestoredCheckpoints,
                    totalNumberCheckpoints,
                    numberInProgressCheckpoints,
                    numberCompletedCheckpoints,
                    numberFailedCheckpoints);
        }
    }

    /** Checkpoint summary. */
    public static final class Summary {

        /**
         * The accurate name of this field should be 'checkpointed_data_size', keep it as before to
         * not break backwards compatibility for old web UI.
         *
         * @see <a href="https://issues.apache.org/jira/browse/FLINK-13390">FLINK-13390</a>
         */
        public static final String FIELD_NAME_STATE_SIZE = "state_size";

        public static final String FIELD_NAME_DURATION = "end_to_end_duration";

        public static final String FIELD_NAME_ALIGNMENT_BUFFERED = "alignment_buffered";

        public static final String FIELD_NAME_PROCESSED_DATA = "processed_data";

        public static final String FIELD_NAME_PERSISTED_DATA = "persisted_data";

        @JsonProperty(FIELD_NAME_STATE_SIZE)
        private final MinMaxAvgStatistics stateSize;

        @JsonProperty(FIELD_NAME_DURATION)
        private final MinMaxAvgStatistics duration;

        @JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED)
        private final MinMaxAvgStatistics alignmentBuffered;

        @JsonProperty(FIELD_NAME_PROCESSED_DATA)
        private final MinMaxAvgStatistics processedData;

        @JsonProperty(FIELD_NAME_PERSISTED_DATA)
        private final MinMaxAvgStatistics persistedData;

        @JsonCreator
        public Summary(
                @JsonProperty(FIELD_NAME_STATE_SIZE) MinMaxAvgStatistics stateSize,
                @JsonProperty(FIELD_NAME_DURATION) MinMaxAvgStatistics duration,
                @JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED) MinMaxAvgStatistics alignmentBuffered,
                @JsonProperty(FIELD_NAME_PROCESSED_DATA) MinMaxAvgStatistics processedData,
                @JsonProperty(FIELD_NAME_PERSISTED_DATA) MinMaxAvgStatistics persistedData) {
            this.stateSize = stateSize;
            this.duration = duration;
            this.alignmentBuffered = alignmentBuffered;
            this.processedData = processedData;
            this.persistedData = persistedData;
        }

        public MinMaxAvgStatistics getStateSize() {
            return stateSize;
        }

        public MinMaxAvgStatistics getDuration() {
            return duration;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Summary summary = (Summary) o;
            return Objects.equals(stateSize, summary.stateSize)
                    && Objects.equals(duration, summary.duration)
                    && Objects.equals(alignmentBuffered, summary.alignmentBuffered)
                    && Objects.equals(processedData, summary.processedData)
                    && Objects.equals(persistedData, summary.persistedData);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    stateSize, duration, alignmentBuffered, processedData, persistedData);
        }
    }

    /** Statistics about the latest checkpoints. */
    public static final class LatestCheckpoints {

        public static final String FIELD_NAME_COMPLETED = "completed";

        public static final String FIELD_NAME_SAVEPOINT = "savepoint";

        public static final String FIELD_NAME_FAILED = "failed";

        public static final String FIELD_NAME_RESTORED = "restored";

        @JsonProperty(FIELD_NAME_COMPLETED)
        @Nullable
        private final CheckpointStatistics.CompletedCheckpointStatistics
                completedCheckpointStatistics;

        @JsonProperty(FIELD_NAME_SAVEPOINT)
        @Nullable
        private final CheckpointStatistics.CompletedCheckpointStatistics savepointStatistics;

        @JsonProperty(FIELD_NAME_FAILED)
        @Nullable
        private final CheckpointStatistics.FailedCheckpointStatistics failedCheckpointStatistics;

        @JsonProperty(FIELD_NAME_RESTORED)
        @Nullable
        private final RestoredCheckpointStatistics restoredCheckpointStatistics;

        @JsonCreator
        public LatestCheckpoints(
                @JsonProperty(FIELD_NAME_COMPLETED) @Nullable
                        CheckpointStatistics.CompletedCheckpointStatistics
                                completedCheckpointStatistics,
                @JsonProperty(FIELD_NAME_SAVEPOINT) @Nullable
                        CheckpointStatistics.CompletedCheckpointStatistics savepointStatistics,
                @JsonProperty(FIELD_NAME_FAILED) @Nullable
                        CheckpointStatistics.FailedCheckpointStatistics failedCheckpointStatistics,
                @JsonProperty(FIELD_NAME_RESTORED) @Nullable
                        RestoredCheckpointStatistics restoredCheckpointStatistics) {
            this.completedCheckpointStatistics = completedCheckpointStatistics;
            this.savepointStatistics = savepointStatistics;
            this.failedCheckpointStatistics = failedCheckpointStatistics;
            this.restoredCheckpointStatistics = restoredCheckpointStatistics;
        }

        @Nullable
        public CheckpointStatistics.CompletedCheckpointStatistics
                getCompletedCheckpointStatistics() {
            return completedCheckpointStatistics;
        }

        @Nullable
        public CheckpointStatistics.CompletedCheckpointStatistics getSavepointStatistics() {
            return savepointStatistics;
        }

        @Nullable
        public CheckpointStatistics.FailedCheckpointStatistics getFailedCheckpointStatistics() {
            return failedCheckpointStatistics;
        }

        @Nullable
        public RestoredCheckpointStatistics getRestoredCheckpointStatistics() {
            return restoredCheckpointStatistics;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LatestCheckpoints that = (LatestCheckpoints) o;
            return Objects.equals(completedCheckpointStatistics, that.completedCheckpointStatistics)
                    && Objects.equals(savepointStatistics, that.savepointStatistics)
                    && Objects.equals(failedCheckpointStatistics, that.failedCheckpointStatistics)
                    && Objects.equals(
                            restoredCheckpointStatistics, that.restoredCheckpointStatistics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    completedCheckpointStatistics,
                    savepointStatistics,
                    failedCheckpointStatistics,
                    restoredCheckpointStatistics);
        }
    }

    /** Statistics for a restored checkpoint. */
    public static final class RestoredCheckpointStatistics {

        public static final String FIELD_NAME_ID = "id";

        public static final String FIELD_NAME_RESTORE_TIMESTAMP = "restore_timestamp";

        public static final String FIELD_NAME_IS_SAVEPOINT = "is_savepoint";

        public static final String FIELD_NAME_EXTERNAL_PATH = "external_path";

        @JsonProperty(FIELD_NAME_ID)
        private final long id;

        @JsonProperty(FIELD_NAME_RESTORE_TIMESTAMP)
        private final long restoreTimestamp;

        @JsonProperty(FIELD_NAME_IS_SAVEPOINT)
        private final boolean savepoint;

        @JsonProperty(FIELD_NAME_EXTERNAL_PATH)
        @Nullable
        private final String externalPath;

        @JsonCreator
        public RestoredCheckpointStatistics(
                @JsonProperty(FIELD_NAME_ID) long id,
                @JsonProperty(FIELD_NAME_RESTORE_TIMESTAMP) long restoreTimestamp,
                @JsonProperty(FIELD_NAME_IS_SAVEPOINT) boolean savepoint,
                @JsonProperty(FIELD_NAME_EXTERNAL_PATH) @Nullable String externalPath) {
            this.id = id;
            this.restoreTimestamp = restoreTimestamp;
            this.savepoint = savepoint;
            this.externalPath = externalPath;
        }

        public long getId() {
            return id;
        }

        public long getRestoreTimestamp() {
            return restoreTimestamp;
        }

        public boolean isSavepoint() {
            return savepoint;
        }

        @Nullable
        public String getExternalPath() {
            return externalPath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RestoredCheckpointStatistics that = (RestoredCheckpointStatistics) o;
            return id == that.id
                    && restoreTimestamp == that.restoreTimestamp
                    && savepoint == that.savepoint
                    && Objects.equals(externalPath, that.externalPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, restoreTimestamp, savepoint, externalPath);
        }
    }
}

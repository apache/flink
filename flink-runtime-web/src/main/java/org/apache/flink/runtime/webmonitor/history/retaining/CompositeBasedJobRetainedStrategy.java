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

package org.apache.flink.runtime.webmonitor.history.retaining;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.HistoryServerOptions.JobArchivedRetainedMode;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS;
import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS_MODE;
import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS_MODE_THRESHOLDS;

/** The composite based retained strategy. */
public class CompositeBasedJobRetainedStrategy implements JobArchivesRetainedStrategy {

    public static JobArchivesRetainedStrategy createFrom(ReadableConfig config) {
        Optional<JobArchivedRetainedMode> retainedModeOpt =
                config.getOptional(HISTORY_SERVER_RETAINED_JOBS_MODE);
        JobArchivedRetainedMode retainedMode;
        if (!retainedModeOpt.isPresent()) {
            int maxHistorySizeByOldKey = config.get(HISTORY_SERVER_RETAINED_JOBS);
            if (maxHistorySizeByOldKey == 0 || maxHistorySizeByOldKey < -1) {
                throw new IllegalConfigurationException(
                        "Cannot set %s to 0 or less than -1", HISTORY_SERVER_RETAINED_JOBS.key());
            }
            return maxHistorySizeByOldKey != HISTORY_SERVER_RETAINED_JOBS.defaultValue()
                    ? new CompositeBasedJobRetainedStrategy(
                            LogicEvaluate.OR,
                            new QuantityBasedJobRetainedStrategy(maxHistorySizeByOldKey))
                    : new CompositeBasedJobRetainedStrategy(LogicEvaluate.OR);
        } else {
            retainedMode = retainedModeOpt.get();
        }

        return getTargetJobRetainedStrategy(config, retainedMode);
    }

    private static CompositeBasedJobRetainedStrategy getTargetJobRetainedStrategy(
            ReadableConfig config, JobArchivedRetainedMode retainedMode) {
        Map<String, String> thresholds = config.get(HISTORY_SERVER_RETAINED_JOBS_MODE_THRESHOLDS);

        switch (retainedMode) {
            case None:
                return new CompositeBasedJobRetainedStrategy(LogicEvaluate.OR);
            case Ttl:
                return new CompositeBasedJobRetainedStrategy(
                        LogicEvaluate.OR, new TimeToLiveBasedJobRetainedStrategy(thresholds));
            case Quantity:
                return new CompositeBasedJobRetainedStrategy(
                        LogicEvaluate.OR, new QuantityBasedJobRetainedStrategy(thresholds));
            case TtlAndQuantity:
                return new CompositeBasedJobRetainedStrategy(
                        LogicEvaluate.AND,
                        new TimeToLiveBasedJobRetainedStrategy(thresholds),
                        new QuantityBasedJobRetainedStrategy(thresholds));
            case TtlOrQuantity:
                return new CompositeBasedJobRetainedStrategy(
                        LogicEvaluate.OR,
                        new TimeToLiveBasedJobRetainedStrategy(thresholds),
                        new QuantityBasedJobRetainedStrategy(thresholds));
            default:
                throw new IllegalConfigurationException(
                        "Unsupported retained mode " + retainedMode);
        }
    }

    /** The enum to represent the logic to evaluate the target retained strategies. */
    enum LogicEvaluate {
        /** Logic OR Rule to evaluate the target strategies. */
        OR,
        /** Logic AND Rule to evaluate the target strategies. */
        AND,
    }

    private final LogicEvaluate logicEvaluate;
    private final List<JobArchivesRetainedStrategy> strategies;

    CompositeBasedJobRetainedStrategy(
            LogicEvaluate logicEvaluate, JobArchivesRetainedStrategy... strategies) {
        this.logicEvaluate = logicEvaluate;
        this.strategies =
                strategies == null || strategies.length == 0
                        ? Collections.emptyList()
                        : Arrays.asList(strategies);
    }

    @Override
    public boolean shouldRetain(FileStatus file, int fileOrderedIndex) {
        if (strategies.isEmpty()) {
            return true;
        }

        if (logicEvaluate == LogicEvaluate.OR) {
            return strategies.stream().anyMatch(s -> s.shouldRetain(file, fileOrderedIndex));
        } else if (logicEvaluate == LogicEvaluate.AND) {
            return strategies.stream().allMatch(s -> s.shouldRetain(file, fileOrderedIndex));
        }
        return true;
    }

    @VisibleForTesting
    public List<JobArchivesRetainedStrategy> getStrategies() {
        return Collections.unmodifiableList(strategies);
    }

    @VisibleForTesting
    public LogicEvaluate getLogicEvaluate() {
        return logicEvaluate;
    }
}

/** The time to live based retained strategy. */
class TimeToLiveBasedJobRetainedStrategy implements JobArchivesRetainedStrategy {

    static final String TTL_KEY = "ttl";

    private final Duration ttlThreshold;

    TimeToLiveBasedJobRetainedStrategy(Map<String, String> thresholds) {
        this.ttlThreshold =
                TimeUtils.parseDuration(
                        thresholds.getOrDefault(
                                TTL_KEY,
                                HISTORY_SERVER_RETAINED_JOBS_MODE_THRESHOLDS
                                        .defaultValue()
                                        .get(TTL_KEY)));
    }

    @Override
    public boolean shouldRetain(FileStatus file, int fileOrderedIndex) {
        if (ttlThreshold == null || ttlThreshold.toMillis() <= 0L) {
            return true;
        }
        return Instant.now().toEpochMilli() - file.getModificationTime() < ttlThreshold.toMillis();
    }
}

/** The job quantity based retained strategy. */
class QuantityBasedJobRetainedStrategy implements JobArchivesRetainedStrategy {

    static final String QUANTITY_KEY = "quantity";

    private final Integer quantityThreshold;

    QuantityBasedJobRetainedStrategy(Map<String, String> thresholds) {
        this.quantityThreshold =
                Integer.parseInt(
                        thresholds.getOrDefault(
                                QUANTITY_KEY,
                                HISTORY_SERVER_RETAINED_JOBS_MODE_THRESHOLDS
                                        .defaultValue()
                                        .get(QUANTITY_KEY)));
    }

    QuantityBasedJobRetainedStrategy(int quantityThreshold) {
        this.quantityThreshold = quantityThreshold;
    }

    @Override
    public boolean shouldRetain(FileStatus file, int fileOrderedIndex) {
        if (quantityThreshold == null || quantityThreshold < 0) {
            return true;
        }
        return quantityThreshold >= fileOrderedIndex;
    }
}

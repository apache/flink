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

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileStatus;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_APPLICATIONS;
import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS;
import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_TTL;
import static org.apache.flink.util.Preconditions.checkArgument;

/** The retained strategy. */
public class CompositeArchiveRetainedStrategy implements ArchiveRetainedStrategy {

    public static ArchiveRetainedStrategy createForJobFromConfig(ReadableConfig config) {
        int maxHistorySizeByOldKey = config.get(HISTORY_SERVER_RETAINED_JOBS);
        if (maxHistorySizeByOldKey == 0 || maxHistorySizeByOldKey < -1) {
            throw new IllegalConfigurationException(
                    "Cannot set %s to 0 or less than -1", HISTORY_SERVER_RETAINED_JOBS.key());
        }
        Optional<Duration> retainedTtlOpt = config.getOptional(HISTORY_SERVER_RETAINED_TTL);
        return new CompositeArchiveRetainedStrategy(
                new QuantityArchiveRetainedStrategy(maxHistorySizeByOldKey),
                new TimeToLiveArchiveRetainedStrategy(retainedTtlOpt.orElse(null)));
    }

    public static ArchiveRetainedStrategy createForApplicationFromConfig(ReadableConfig config) {
        int maxHistorySize = config.get(HISTORY_SERVER_RETAINED_APPLICATIONS);
        if (maxHistorySize == 0 || maxHistorySize < -1) {
            throw new IllegalConfigurationException(
                    "Cannot set %s to 0 or less than -1",
                    HISTORY_SERVER_RETAINED_APPLICATIONS.key());
        }
        Optional<Duration> retainedTtlOpt = config.getOptional(HISTORY_SERVER_RETAINED_TTL);
        return new CompositeArchiveRetainedStrategy(
                new QuantityArchiveRetainedStrategy(maxHistorySize),
                new TimeToLiveArchiveRetainedStrategy(retainedTtlOpt.orElse(null)));
    }

    private final List<ArchiveRetainedStrategy> strategies;

    CompositeArchiveRetainedStrategy(ArchiveRetainedStrategy... strategies) {
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
        return strategies.stream().allMatch(s -> s.shouldRetain(file, fileOrderedIndex));
    }
}

/** The time to live based retained strategy. */
class TimeToLiveArchiveRetainedStrategy implements ArchiveRetainedStrategy {

    @Nullable private final Duration ttlThreshold;

    TimeToLiveArchiveRetainedStrategy(@Nullable Duration ttlThreshold) {
        if (ttlThreshold != null && ttlThreshold.toMillis() <= 0) {
            throw new IllegalConfigurationException(
                    "Cannot set %s to 0 or less than 0 milliseconds",
                    HISTORY_SERVER_RETAINED_TTL.key());
        }
        this.ttlThreshold = ttlThreshold;
    }

    @Override
    public boolean shouldRetain(FileStatus file, int fileOrderedIndex) {
        if (ttlThreshold == null) {
            return true;
        }
        return Instant.now().toEpochMilli() - file.getModificationTime() < ttlThreshold.toMillis();
    }
}

/** The quantity based retained strategy. */
class QuantityArchiveRetainedStrategy implements ArchiveRetainedStrategy {

    private final int quantityThreshold;

    QuantityArchiveRetainedStrategy(int quantityThreshold) {
        checkArgument(quantityThreshold == -1 || quantityThreshold > 0);
        this.quantityThreshold = quantityThreshold;
    }

    @Override
    public boolean shouldRetain(FileStatus file, int fileOrderedIndex) {
        if (quantityThreshold == -1) {
            return true;
        }
        return fileOrderedIndex <= quantityThreshold;
    }
}

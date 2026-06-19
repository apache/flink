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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/** Aggregated info of a set of tasks. */
public class AggregatedTaskDetailsInfo implements ResponseBody {
    public static final String FIELD_NAME_METRICS = "metrics";
    public static final String FIELD_NAME_STATUS_DURATION = "status-duration";

    private static final String[] metricsNames = {
        IOMetricsInfo.FIELD_NAME_BYTES_READ,
        IOMetricsInfo.FIELD_NAME_BYTES_WRITTEN,
        IOMetricsInfo.FIELD_NAME_RECORDS_READ,
        IOMetricsInfo.FIELD_NAME_RECORDS_WRITTEN,
        IOMetricsInfo.FIELD_NAME_ACC_BACK_PRESSURE,
        IOMetricsInfo.FIELD_NAME_ACC_BUSY,
        IOMetricsInfo.FIELD_NAME_ACC_IDLE
    };

    private static final String[] statusNames = {
        ExecutionState.CREATED.name(),
        ExecutionState.SCHEDULED.name(),
        ExecutionState.INITIALIZING.name(),
        ExecutionState.DEPLOYING.name(),
        ExecutionState.RUNNING.name()
    };

    @JsonProperty(FIELD_NAME_METRICS)
    private final Map<String, Map<String, Long>> metrics;

    @JsonProperty(FIELD_NAME_STATUS_DURATION)
    private final Map<String, Map<String, Long>> statusDuration;

    @JsonCreator
    public AggregatedTaskDetailsInfo(
            @JsonProperty(FIELD_NAME_METRICS) Map<String, Map<String, Long>> metrics,
            @JsonProperty(FIELD_NAME_STATUS_DURATION)
                    Map<String, Map<String, Long>> statusDuration) {
        this.metrics = Preconditions.checkNotNull(metrics);
        this.statusDuration = Preconditions.checkNotNull(statusDuration);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AggregatedTaskDetailsInfo that = (AggregatedTaskDetailsInfo) o;
        return Objects.equals(metrics, that.metrics)
                && Objects.equals(statusDuration, that.statusDuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metrics, statusDuration);
    }

    public static AggregatedTaskDetailsInfo create(
            List<SubtaskExecutionAttemptDetailsInfo> subtaskInfo) {
        return create(
                subtaskInfo.stream()
                        .map(SubtaskExecutionAttemptDetailsInfo::getIoMetricsInfo)
                        .collect(Collectors.toList()),
                subtaskInfo.stream()
                        .map(SubtaskExecutionAttemptDetailsInfo::getStatusDuration)
                        .collect(Collectors.toList()));
    }

    public static AggregatedTaskDetailsInfo create(
            List<IOMetricsInfo> ioMetricsInfos, List<Map<ExecutionState, Long>> statusDurations) {
        Map<String, MetricsStatistics> metrics = new HashMap<>();
        Map<String, MetricsStatistics> statusDuration = new HashMap<>();
        Arrays.stream(metricsNames).forEach(name -> metrics.put(name, new MetricsStatistics(name)));
        Arrays.stream(statusNames)
                .forEach(name -> statusDuration.put(name, new MetricsStatistics(name)));

        ioMetricsInfos.forEach(
                ioMetricsInfo -> {
                    metrics.get(IOMetricsInfo.FIELD_NAME_BYTES_READ)
                            .addValue(ioMetricsInfo.getBytesRead());
                    metrics.get(IOMetricsInfo.FIELD_NAME_BYTES_WRITTEN)
                            .addValue(ioMetricsInfo.getBytesWritten());
                    metrics.get(IOMetricsInfo.FIELD_NAME_RECORDS_READ)
                            .addValue(ioMetricsInfo.getRecordsRead());
                    metrics.get(IOMetricsInfo.FIELD_NAME_RECORDS_WRITTEN)
                            .addValue(ioMetricsInfo.getRecordsWritten());
                    metrics.get(IOMetricsInfo.FIELD_NAME_ACC_IDLE)
                            .addValue(ioMetricsInfo.getAccumulatedIdle());
                    metrics.get(IOMetricsInfo.FIELD_NAME_ACC_BACK_PRESSURE)
                            .addValue(ioMetricsInfo.getAccumulatedBackpressured());
                    if (!Double.isNaN(ioMetricsInfo.getAccumulatedBusy())) {
                        metrics.get(IOMetricsInfo.FIELD_NAME_ACC_BUSY)
                                .addValue((long) ioMetricsInfo.getAccumulatedBusy());
                    } else {
                        metrics.get(IOMetricsInfo.FIELD_NAME_ACC_BUSY).addValue(-1);
                    }
                });

        statusDurations.forEach(
                status -> {
                    statusDuration
                            .get(ExecutionState.CREATED.name())
                            .addValue(status.get(ExecutionState.CREATED));
                    statusDuration
                            .get(ExecutionState.SCHEDULED.name())
                            .addValue(status.get(ExecutionState.SCHEDULED));
                    statusDuration
                            .get(ExecutionState.INITIALIZING.name())
                            .addValue(status.get(ExecutionState.INITIALIZING));
                    statusDuration
                            .get(ExecutionState.DEPLOYING.name())
                            .addValue(status.get(ExecutionState.DEPLOYING));
                    statusDuration
                            .get(ExecutionState.RUNNING.name())
                            .addValue(status.get(ExecutionState.RUNNING));
                });
        return new AggregatedTaskDetailsInfo(
                metrics.values().stream()
                        .collect(
                                Collectors.toMap(
                                        MetricsStatistics::getName, MetricsStatistics::toMap)),
                statusDuration.values().stream()
                        .collect(
                                Collectors.toMap(
                                        MetricsStatistics::getName, MetricsStatistics::toMap)));
    }

    @VisibleForTesting
    static class MetricsStatistics {
        private final List<Long> values = new ArrayList<>();
        private final String name;
        private long sum = 0;
        private Percentile percentile = null;

        MetricsStatistics(String name) {
            this.name = name;
        }

        void addValue(long value) {
            values.add(value);
            sum += value;
        }

        private String getName() {
            return name;
        }

        private Map<String, Long> toMap() {
            Map<String, Long> result = new HashMap<>();
            result.put("min", getMin());
            result.put("max", getMax());
            result.put("avg", getAvg());
            result.put("sum", getSum());
            result.put("median", getPercentile(50));
            result.put("p25", getPercentile(25));
            result.put("p75", getPercentile(75));
            result.put("p95", getPercentile(95));
            return result;
        }

        long getMin() {
            return values.stream()
                    .reduce(BinaryOperator.minBy(Comparator.naturalOrder()))
                    .orElse(0L);
        }

        long getMax() {
            return values.stream()
                    .reduce(BinaryOperator.maxBy(Comparator.naturalOrder()))
                    .orElse(0L);
        }

        long getSum() {
            return sum;
        }

        long getAvg() {
            return values.isEmpty() ? 0 : sum / values.size();
        }

        long getPercentile(int percent) {
            if (percentile == null) {
                percentile = new Percentile();
                percentile.setData(values.stream().mapToDouble(Long::doubleValue).toArray());
            }
            return (long) percentile.evaluate(percent);
        }
    }
}

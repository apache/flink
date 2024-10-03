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

package org.apache.flink.metrics.otel;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.export.MetricProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.metrics.otel.OpenTelemetryReporterOptions.tryConfigureEndpoint;
import static org.apache.flink.metrics.otel.OpenTelemetryReporterOptions.tryConfigureTimeout;

/**
 * A Flink {@link org.apache.flink.metrics.reporter.MetricReporter} which is made to export metrics
 * using Open Telemetry's {@link MetricExporter}.
 */
public class OpenTelemetryMetricReporter extends OpenTelemetryReporterBase
        implements MetricReporter, MetricProducer, Scheduled {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryMetricReporter.class);

    private static final String LOGICAL_SCOPE_PREFIX = "flink.";

    private final Map<Gauge<?>, MetricMetadata> gauges = new HashMap<>();
    private final Map<Counter, MetricMetadata> counters = new HashMap<>();
    private final Map<Histogram, MetricMetadata> histograms = new HashMap<>();
    private final Map<Meter, MetricMetadata> meters = new HashMap<>();
    private final Clock clock;

    // In order to produce deltas, we keep a snapshot of the previous counter collection.
    private Map<Metric, Long> lastValueSnapshots = Collections.emptyMap();
    private long lastCollectTimeNanos = 0;

    public OpenTelemetryMetricReporter() {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    OpenTelemetryMetricReporter(Clock clock) {
        this.clock = clock;
    }

    @Override
    public void open(MetricConfig metricConfig) {
        LOG.info("Starting OpenTelemetryMetricReporter");
        super.open(metricConfig);
        OtlpGrpcMetricExporterBuilder builder = OtlpGrpcMetricExporter.builder();
        tryConfigureEndpoint(metricConfig, builder::setEndpoint);
        tryConfigureTimeout(metricConfig, builder::setTimeout);
        exporter = builder.build();
    }

    @Override
    public void close() {
        exporter.flush();
        lastResult.join(1, TimeUnit.MINUTES);
        exporter.close();
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final String name =
                LOGICAL_SCOPE_PREFIX
                        + LogicalScopeProvider.castFrom(group)
                                .getLogicalScope(CharacterFilter.NO_OP_FILTER)
                        + "."
                        + metricName;

        Map<String, String> variables =
                group.getAllVariables().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> VariableNameUtil.getVariableName(e.getKey()),
                                        Entry::getValue));
        LOG.debug("Adding metric {} with variables {}", metricName, variables);

        MetricMetadata metricMetadata = new MetricMetadata(name, variables);

        synchronized (this) {
            switch (metric.getMetricType()) {
                case COUNTER:
                    this.counters.put((Counter) metric, metricMetadata);
                    break;
                case GAUGE:
                    this.gauges.put((Gauge<?>) metric, metricMetadata);
                    break;
                case HISTOGRAM:
                    this.histograms.put((Histogram) metric, metricMetadata);
                    break;
                case METER:
                    this.meters.put((Meter) metric, metricMetadata);
                    break;
                default:
                    LOG.warn(
                            "Cannot add unknown metric type {}. This indicates that the reporter does not "
                                    + "support this metric type.",
                            metric.getClass().getName());
            }
        }
    }

    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            // Make sure we're not caching the metric object
            lastValueSnapshots.remove(metric);
            switch (metric.getMetricType()) {
                case COUNTER:
                    this.counters.remove((Counter) metric);
                    break;
                case GAUGE:
                    this.gauges.remove((Gauge<?>) metric);
                    break;
                case HISTOGRAM:
                    this.histograms.remove((Histogram) metric);
                    break;
                case METER:
                    this.meters.remove((Meter) metric);
                    break;
                default:
                    LOG.warn(
                            "Cannot remove unknown metric type {}. This indicates that the reporter does "
                                    + "not support this metric type.",
                            metric.getClass().getName());
            }
        }
    }

    private long getCurrentTimeNanos() {
        Instant now = clock.instant();
        return TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano();
    }

    /**
     * Note that all of the metric data structures in {@link AbstractReporter} are guarded by this,
     * so must make this synchronized.
     *
     * @return The collection of metrics
     */
    @Override
    public synchronized Collection<MetricData> collectAllMetrics() {
        long currentTimeNanos = getCurrentTimeNanos();
        List<MetricData> data = new ArrayList<>();
        OpenTelemetryMetricAdapter.CollectionMetadata collectionMetadata =
                new OpenTelemetryMetricAdapter.CollectionMetadata(
                        resource, lastCollectTimeNanos, currentTimeNanos);
        Map<Metric, Long> currentValueSnapshots = takeLastValueSnapshots();
        for (Counter counter : counters.keySet()) {
            Long count = currentValueSnapshots.get(counter);
            Long lastCount = lastValueSnapshots.getOrDefault(counter, 0L);
            MetricMetadata metricMetadata = counters.get(counter);
            Optional<MetricData> metricData =
                    OpenTelemetryMetricAdapter.convertCounter(
                            collectionMetadata, count, lastCount, metricMetadata);
            metricData.ifPresent(data::add);
        }
        for (Gauge<?> gauge : gauges.keySet()) {
            MetricMetadata metricMetadata = gauges.get(gauge);
            Optional<MetricData> metricData =
                    OpenTelemetryMetricAdapter.convertGauge(
                            collectionMetadata, gauge, metricMetadata);
            metricData.ifPresent(data::add);
        }
        for (Meter meter : meters.keySet()) {
            Long count = currentValueSnapshots.get(meter);
            Long lastCount = lastValueSnapshots.getOrDefault(meter, 0L);
            MetricMetadata metricMetadata = meters.get(meter);
            List<MetricData> metricData =
                    OpenTelemetryMetricAdapter.convertMeter(
                            collectionMetadata, meter, count, lastCount, metricMetadata);
            data.addAll(metricData);
        }
        for (Histogram histogram : histograms.keySet()) {
            MetricMetadata metricMetadata = histograms.get(histogram);
            Optional<MetricData> metricData =
                    OpenTelemetryMetricAdapter.convertHistogram(
                            collectionMetadata, histogram, metricMetadata);
            metricData.ifPresent(data::add);
        }
        lastValueSnapshots = currentValueSnapshots;
        lastCollectTimeNanos = currentTimeNanos;
        return data;
    }

    private Map<Metric, Long> takeLastValueSnapshots() {
        Map<Metric, Long> map = new HashMap<>();
        for (Counter counter : counters.keySet()) {
            map.put(counter, counter.getCount());
        }
        for (Meter meter : meters.keySet()) {
            map.put(meter, meter.getCount());
        }
        return map;
    }

    private @Nullable CompletableResultCode lastResult;

    @Override
    public void report() {
        Collection<MetricData> metricData = collectAllMetrics();
        try {
            lastResult = exporter.export(metricData);
            lastResult.whenComplete(
                    () -> {
                        if (lastResult.isSuccess()) {
                            LOG.debug(
                                    "Exported {} metrics using {}",
                                    metricData.size(),
                                    exporter.getClass().getName());
                        } else {
                            LOG.warn(
                                    "Failed to export {} metrics using {}",
                                    metricData.size(),
                                    exporter.getClass().getName());
                        }
                    });
        } catch (Exception e) {
            LOG.error(
                    "Failed to call export for {} metrics using {}",
                    metricData.size(),
                    exporter.getClass().getName());
        }
    }
}

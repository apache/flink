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

import org.apache.flink.shaded.guava33.com.google.common.collect.Lists;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.export.MetricProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.metrics.otel.OpenTelemetryReporterOptions.tryConfigureCompression;
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

    @GuardedBy("this")
    private final Map<Gauge<?>, MetricMetadata> gauges = new HashMap<>();

    @GuardedBy("this")
    private final Map<Counter, MetricMetadata> counters = new HashMap<>();

    @GuardedBy("this")
    private final Map<Histogram, MetricMetadata> histograms = new HashMap<>();

    @GuardedBy("this")
    private final Map<Meter, MetricMetadata> meters = new HashMap<>();

    private final Clock clock;

    // In order to produce deltas, we keep a snapshot of the previous counter collection.
    @GuardedBy("this")
    private Map<Metric, Long> lastValueSnapshots = Collections.emptyMap();

    @GuardedBy("this")
    private long lastCollectTimeNanos = 0;

    @GuardedBy("this")
    private int batchSize = OpenTelemetryReporterOptions.BATCH_SIZE.defaultValue();

    @GuardedBy("this")
    private long exportCompletionTimeoutMillis =
            OpenTelemetryReporterOptions.EXPORT_COMPLETION_TIMEOUT_MILLIS.defaultValue();

    public OpenTelemetryMetricReporter() {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    OpenTelemetryMetricReporter(Clock clock) {
        this.clock = clock;
    }

    @Override
    public void open(final MetricConfig metricConfig) {
        LOG.info("Starting OpenTelemetryMetricReporter");
        super.open(metricConfig);

        synchronized (this) {
            exporter = createExporter(metricConfig);
            configureBatching(metricConfig);
        }
    }

    /** Creates the {@link MetricExporter} based on the configured protocol. */
    protected MetricExporter createExporter(final MetricConfig metricConfig) {
        final String protocol =
                Optional.ofNullable(
                                metricConfig.getProperty(
                                        OpenTelemetryReporterOptions.EXPORTER_PROTOCOL.key()))
                        .orElse("");

        switch (protocol.toLowerCase()) {
            case "http":
                final OtlpHttpMetricExporterBuilder httpBuilder = OtlpHttpMetricExporter.builder();
                tryConfigureEndpoint(metricConfig, httpBuilder::setEndpoint);
                tryConfigureTimeout(metricConfig, httpBuilder::setTimeout);
                tryConfigureCompression(metricConfig, httpBuilder::setCompression);
                return httpBuilder.build();
            default:
                LOG.warn(
                        "Unknown protocol '{}' for OpenTelemetryMetricReporter, defaulting to gRPC",
                        protocol);
            // Fall through to the "gRPC" case
            case "grpc":
                final OtlpGrpcMetricExporterBuilder grpcBuilder = OtlpGrpcMetricExporter.builder();
                tryConfigureEndpoint(metricConfig, grpcBuilder::setEndpoint);
                tryConfigureTimeout(metricConfig, grpcBuilder::setTimeout);
                tryConfigureCompression(metricConfig, grpcBuilder::setCompression);
                return grpcBuilder.build();
        }
    }

    @Override
    public synchronized void close() {
        if (exporter != null) {
            exporter.flush();
            waitForLastReportToComplete();
            exporter.close();
        }
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

    private volatile @Nullable CompletableFuture<Void> lastReportFuture;

    @Override
    public void report() {
        final List<MetricData> metricData = List.copyOf(collectAllMetrics());
        final int totalMetrics = metricData.size();
        if (totalMetrics == 0) {
            return;
        }

        // In order to avoid potentially large memory allocations on `.partition()` call.
        // doesn't require additional synchronized as it comes after collectAllMetrics, which
        // is synchronized
        final int localBatchSize = Math.min(batchSize, totalMetrics);

        final Iterable<List<MetricData>> batches = Lists.partition(metricData, localBatchSize);
        final int totalBatches = (totalMetrics + localBatchSize - 1) / localBatchSize;
        final List<CompletableResultCode> results = new ArrayList<>(totalBatches);
        for (final List<MetricData> batch : batches) {
            results.add(exportBatch(batch));
        }

        lastReportFuture =
                CompletableFuture.runAsync(
                                () ->
                                        reportWhenExportIsComplete(
                                                totalMetrics, results, localBatchSize))
                        .whenComplete(
                                (ignored, throwable) -> {
                                    if (throwable != null) {
                                        LOG.warn("Error while reporting metrics", throwable);
                                    }
                                });
    }

    private CompletableResultCode exportBatch(final Collection<MetricData> batch) {
        try {
            // exporter used without synchronize block, because `exportBatch` happens
            // after `collectAllMetrics` which adds memory barrier.
            return exporter.export(batch);
        } catch (Exception e) {
            LOG.error(
                    "Failed to call export for {} metrics using {}",
                    batch.size(),
                    exporter.getClass().getName(),
                    e);
            return CompletableResultCode.ofFailure();
        }
    }

    private void reportWhenExportIsComplete(
            final int totalMetrics,
            final List<CompletableResultCode> results,
            final int localBatchSize) {
        final CountDownLatch completedResults = new CountDownLatch(results.size());
        results.forEach(result -> result.whenComplete(completedResults::countDown));

        try {
            final boolean isCompleteReport =
                    completedResults.await(exportCompletionTimeoutMillis, TimeUnit.MILLISECONDS);

            final int successfulBatches =
                    (int) results.stream().filter(CompletableResultCode::isSuccess).count();
            final int failedBatches = results.size() - successfulBatches;

            logFinalStatistics(
                    localBatchSize,
                    successfulBatches,
                    failedBatches,
                    totalMetrics,
                    isCompleteReport);
        } catch (final InterruptedException e) {
            LOG.warn(
                    "Thread waiting for metrics export results have been interrupted. "
                            + "Exports results are unknown",
                    e);
            Thread.currentThread().interrupt();
        }
    }

    private void logFinalStatistics(
            int localBatchSize,
            int successfulBatches,
            int failedBatches,
            int totalMetrics,
            boolean isCompleteReport) {
        if (failedBatches > 0 || !isCompleteReport) {
            LOG.warn(
                    "Metric export completed with issues: totalMetrics={}, batchSize={}, "
                            + "successfulBatches={}, failedBatches={}, completedInTime={}",
                    totalMetrics,
                    localBatchSize,
                    successfulBatches,
                    failedBatches,
                    isCompleteReport);
        } else {
            LOG.debug(
                    "Metric export completed successfully: totalMetrics={}, batchSize={}, "
                            + "successfulBatches={}",
                    totalMetrics,
                    localBatchSize,
                    successfulBatches);
        }
    }

    private void configureBatching(MetricConfig metricConfig) {
        int configuredBatchSize =
                metricConfig.getInteger(
                        OpenTelemetryReporterOptions.BATCH_SIZE.key(),
                        OpenTelemetryReporterOptions.BATCH_SIZE.defaultValue());
        batchSize = configuredBatchSize <= 0 ? Integer.MAX_VALUE : configuredBatchSize;
        exportCompletionTimeoutMillis =
                metricConfig.getLong(
                        OpenTelemetryReporterOptions.EXPORT_COMPLETION_TIMEOUT_MILLIS.key(),
                        OpenTelemetryReporterOptions.EXPORT_COMPLETION_TIMEOUT_MILLIS
                                .defaultValue());
        LOG.info(
                "Configured batching: batchSize={}, exportCompletionTimeoutMillis={}",
                batchSize,
                exportCompletionTimeoutMillis);
    }

    @VisibleForTesting
    void waitForLastReportToComplete() {
        if (lastReportFuture != null) {
            try {
                lastReportFuture.get(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                LOG.warn("Waiting for last report to complete failed", e);
            }
        }
    }
}

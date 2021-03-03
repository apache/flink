/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Snapshot;
import org.rocksdb.ColumnFamilyHandle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor.COLUMN_FAMILY_KEY;

/** Metrics to counting access latency. */
public class RocksDBAccessMetric implements AutoCloseable {
    private final Map<Integer, MetricGroup> columnFamilyMetricGroups;

    private final Map<Integer, Counters> countersPerColumnFamily;

    private final Map<Integer, Map<String, Histogram>> histogramMetrics;

    private final MetricGroup metricGroup;

    private final int sampleCountInterval;

    private final boolean metricsSampleEnabled;

    private final long histogramWindowSize;

    private final Supplier<com.codahale.metrics.Histogram> histogramSupplier;

    static final String GET_LATENCY = "getLatency";
    static final String PUT_LATENCY = "putLatency";
    static final String WRITE_BATCH_LATENCY = "writeBatchLatency";
    static final String DELETE_LATENCY = "deleteLatency";
    static final String MERGE_LATENCY = "mergeLatency";
    static final String SEEK_LATENCY = "seekLatency";
    static final String NEXT_LATENCY = "nextLatency";

    RocksDBAccessMetric(
            MetricGroup metricGroup, int sampleCountInterval, long histogramWindowSize) {
        this.metricGroup = Preconditions.checkNotNull(metricGroup);
        this.sampleCountInterval = sampleCountInterval;
        this.metricsSampleEnabled = sampleCountInterval > 1;

        Preconditions.checkArgument(histogramWindowSize > 0);
        this.histogramWindowSize = histogramWindowSize;
        this.histogramMetrics = new HashMap<>();
        this.columnFamilyMetricGroups = new HashMap<>();
        this.countersPerColumnFamily = new HashMap<>();
        this.histogramSupplier =
                () ->
                        new com.codahale.metrics.Histogram(
                                new SlidingTimeWindowReservoir(
                                        histogramWindowSize, TimeUnit.SECONDS));
    }

    boolean checkAndUpdateGetCounter(final int columnFamilyHandleId) {
        return metricsSampleEnabled
                && countersPerColumnFamily.get(columnFamilyHandleId).checkAndUpdateGetCounter();
    }

    boolean checkAndUpdatePutCounter(final int columnFamilyHandleId) {
        return metricsSampleEnabled
                && countersPerColumnFamily.get(columnFamilyHandleId).checkAndUpdatePutCounter();
    }

    boolean checkAndUpdateWriteBatchCounter(final int columnFamilyHandleId) {
        return metricsSampleEnabled
                && countersPerColumnFamily
                        .get(columnFamilyHandleId)
                        .checkAndUpdateWriteBatchCounter();
    }

    boolean checkAndUpdateDeleteCounter(final int columnFamilyHandleId) {
        return metricsSampleEnabled
                && countersPerColumnFamily.get(columnFamilyHandleId).checkAndUpdateDeleteCounter();
    }

    boolean checkAndUpdateMergeCounter(final int columnFamilyHandleId) {
        return metricsSampleEnabled
                && countersPerColumnFamily.get(columnFamilyHandleId).checkAndUpdateMergeCounter();
    }

    boolean checkAndUpdateSeekCounter(final int columnFamilyHandleId) {
        return metricsSampleEnabled
                && countersPerColumnFamily.get(columnFamilyHandleId).checkAndUpdateSeekCounter();
    }

    boolean checkAndUpdateNextCounter(final int columnFamilyHandleId) {
        return metricsSampleEnabled
                && countersPerColumnFamily.get(columnFamilyHandleId).checkAndUpdateNextCounter();
    }

    public void updateHistogram(
            final int columnFamilyHandleId, final String metricName, final long durationNanoTime) {
        this.histogramMetrics
                .get(columnFamilyHandleId)
                .computeIfAbsent(
                        metricName,
                        (k) -> {
                            HistogramWrapper histogram =
                                    new HistogramWrapper(histogramSupplier.get());
                            columnFamilyMetricGroups
                                    .get(columnFamilyHandleId)
                                    .histogram(metricName, histogram);
                            return histogram;
                        })
                .update(durationNanoTime);
    }

    boolean isMetricsSampleEnabled() {
        return metricsSampleEnabled;
    }

    int getSampleCountInterval() {
        return sampleCountInterval;
    }

    long getHistogramWindowSize() {
        return histogramWindowSize;
    }

    @VisibleForTesting
    Map<Integer, Map<String, Histogram>> getHistogramMetrics() {
        return histogramMetrics;
    }

    /**
     * Register histogram to track latency metrics for the column family.
     *
     * @param columnFamilyName group name for the new gauges
     * @param handle native handle to the column family
     */
    void registerColumnFamily(String columnFamilyName, ColumnFamilyHandle handle) {
        int columnFamilyId = handle.getID();
        columnFamilyMetricGroups.putIfAbsent(
                columnFamilyId, metricGroup.addGroup(COLUMN_FAMILY_KEY, columnFamilyName));
        histogramMetrics.putIfAbsent(columnFamilyId, new HashMap<>());
        if (metricsSampleEnabled) {
            countersPerColumnFamily.putIfAbsent(columnFamilyId, new Counters(sampleCountInterval));
        }
    }

    @Override
    public void close() {
        this.histogramMetrics.clear();
        this.countersPerColumnFamily.clear();
        this.columnFamilyMetricGroups.clear();
    }

    static class HistogramWrapper implements Histogram {
        private final com.codahale.metrics.Histogram histogram;

        public HistogramWrapper(com.codahale.metrics.Histogram histogram) {
            this.histogram = histogram;
        }

        @Override
        public void update(long value) {
            histogram.update(value);
        }

        @Override
        public long getCount() {
            return histogram.getCount();
        }

        @Override
        public HistogramStatistics getStatistics() {
            return new SnapshotHistogramStatistics(this.histogram.getSnapshot());
        }
    }

    private static class SnapshotHistogramStatistics extends HistogramStatistics {

        private final Snapshot snapshot;

        SnapshotHistogramStatistics(com.codahale.metrics.Snapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public double getQuantile(double quantile) {
            return snapshot.getValue(quantile);
        }

        @Override
        public long[] getValues() {
            return snapshot.getValues();
        }

        @Override
        public int size() {
            return snapshot.size();
        }

        @Override
        public double getMean() {
            return snapshot.getMean();
        }

        @Override
        public double getStdDev() {
            return snapshot.getStdDev();
        }

        @Override
        public long getMax() {
            return snapshot.getMax();
        }

        @Override
        public long getMin() {
            return snapshot.getMin();
        }
    }

    public static Builder builderFromConfig(ReadableConfig config) {
        Builder builder = new Builder();
        return builder.setEnabled(config.get(RocksDBOptions.LATENCY_TRACK_ENABLED))
                .setSampleInterval(config.get(RocksDBOptions.LATENCY_TRACK_SAMPLE_INTERVAL))
                .setHistogramSlidingWindow(config.get(RocksDBOptions.LATENCY_TRACK_SLIDING_WINDOW));
    }

    /** Builder for {@link RocksDBAccessMetric}. */
    public static class Builder implements Serializable {
        private static final long serialVersionUID = 1L;

        private boolean isEnabled = RocksDBOptions.LATENCY_TRACK_ENABLED.defaultValue();
        private int sampleInterval = RocksDBOptions.LATENCY_TRACK_SAMPLE_INTERVAL.defaultValue();
        private long histogramSlidingWindow =
                RocksDBOptions.LATENCY_TRACK_SLIDING_WINDOW.defaultValue();
        private MetricGroup metricGroup;

        public Builder setEnabled(boolean enabled) {
            this.isEnabled = enabled;
            return this;
        }

        public Builder setSampleInterval(int sampleInterval) {
            this.sampleInterval = sampleInterval;
            return this;
        }

        public Builder setHistogramSlidingWindow(long histogramSlidingWindow) {
            this.histogramSlidingWindow = histogramSlidingWindow;
            return this;
        }

        public Builder setMetricGroup(MetricGroup metricGroup) {
            this.metricGroup = metricGroup;
            return this;
        }

        public RocksDBAccessMetric build() {
            if (isEnabled) {
                return new RocksDBAccessMetric(
                        Preconditions.checkNotNull(metricGroup),
                        sampleInterval,
                        histogramSlidingWindow);
            } else {
                return null;
            }
        }
    }

    private static class Counters {
        private final int metricSampledInterval;
        private int getCounter;
        private int putCounter;
        private int writeBatchCounter;
        private int deleteCounter;
        private int mergeCounter;
        private int seekCounter;
        private int nextCounter;

        Counters(int metricSampledInterval) {
            this.metricSampledInterval = metricSampledInterval;
            this.getCounter = 0;
            this.putCounter = 0;
            this.writeBatchCounter = 0;
            this.deleteCounter = 0;
            this.mergeCounter = 0;
            this.seekCounter = 0;
            this.nextCounter = 0;
        }

        private int updateMetricsSampledCounter(int counter) {
            return (counter + 1 < metricSampledInterval) ? counter + 1 : 0;
        }

        boolean checkAndUpdateGetCounter() {
            boolean result = getCounter == 0;
            this.getCounter = updateMetricsSampledCounter(getCounter);
            return result;
        }

        boolean checkAndUpdatePutCounter() {
            boolean result = putCounter == 0;
            this.putCounter = updateMetricsSampledCounter(putCounter);
            return result;
        }

        boolean checkAndUpdateWriteBatchCounter() {
            boolean result = writeBatchCounter == 0;
            this.writeBatchCounter = updateMetricsSampledCounter(writeBatchCounter);
            return result;
        }

        boolean checkAndUpdateDeleteCounter() {
            boolean result = deleteCounter == 0;
            this.deleteCounter = updateMetricsSampledCounter(deleteCounter);
            return result;
        }

        boolean checkAndUpdateMergeCounter() {
            boolean result = mergeCounter == 0;
            this.mergeCounter = updateMetricsSampledCounter(mergeCounter);
            return result;
        }

        boolean checkAndUpdateSeekCounter() {
            boolean result = seekCounter == 0;
            this.seekCounter = updateMetricsSampledCounter(seekCounter);
            return result;
        }

        boolean checkAndUpdateNextCounter() {
            boolean result = nextCounter == 0;
            this.nextCounter = updateMetricsSampledCounter(nextCounter);
            return result;
        }
    }
}

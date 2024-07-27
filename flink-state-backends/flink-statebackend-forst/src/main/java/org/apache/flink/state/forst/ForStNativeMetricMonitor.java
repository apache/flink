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

package org.apache.flink.state.forst;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.math.BigInteger;

/**
 * A monitor which pulls {{@link RocksDB}} native metrics and forwards them to Flink's metric group.
 * All metrics are unsigned longs and are reported at the column family level.
 */
@Internal
public class ForStNativeMetricMonitor implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ForStNativeMetricMonitor.class);

    private final ForStNativeMetricOptions options;

    private final MetricGroup metricGroup;

    private final Object lock;

    static final String COLUMN_FAMILY_KEY = "column_family";

    @GuardedBy("lock")
    private RocksDB rocksDB;

    @Nullable
    @GuardedBy("lock")
    private Statistics statistics;

    public ForStNativeMetricMonitor(
            @Nonnull ForStNativeMetricOptions options,
            @Nonnull MetricGroup metricGroup,
            @Nonnull RocksDB rocksDB,
            @Nullable Statistics statistics) {
        this.options = options;
        this.metricGroup = metricGroup;
        this.rocksDB = rocksDB;
        this.statistics = statistics;
        this.lock = new Object();
        registerStatistics();
    }

    /** Register gauges to pull native metrics for the database. */
    private void registerStatistics() {
        if (statistics != null) {
            for (TickerType tickerType : options.getMonitorTickerTypes()) {
                metricGroup.gauge(
                        String.format("rocksdb.%s", tickerType.name().toLowerCase()),
                        new ForStNativeStatisticsMetricView(tickerType));
            }
        }
    }

    /**
     * Register gauges to pull native metrics for the column family.
     *
     * @param columnFamilyName group name for the new gauges
     * @param handle native handle to the column family
     */
    void registerColumnFamily(String columnFamilyName, ColumnFamilyHandle handle) {

        boolean columnFamilyAsVariable = options.isColumnFamilyAsVariable();
        MetricGroup group =
                columnFamilyAsVariable
                        ? metricGroup.addGroup(COLUMN_FAMILY_KEY, columnFamilyName)
                        : metricGroup.addGroup(columnFamilyName);

        for (ForStProperty property : options.getProperties()) {
            ForStNativePropertyMetricView gauge =
                    new ForStNativePropertyMetricView(handle, property);
            group.gauge(property.getForStProperty(), gauge);
        }
    }

    /** Updates the value of metricView if the reference is still valid. */
    private void setProperty(ForStNativePropertyMetricView metricView) {
        if (metricView.isClosed()) {
            return;
        }
        try {
            synchronized (lock) {
                if (rocksDB != null) {
                    long value =
                            metricView.property.getNumericalPropertyValue(
                                    rocksDB, metricView.handle);
                    metricView.setValue(value);
                }
            }
        } catch (Exception e) {
            metricView.close();
            LOG.warn("Failed to read native metric {} from ForSt.", metricView.property, e);
        }
    }

    private void setStatistics(ForStNativeStatisticsMetricView metricView) {
        if (metricView.isClosed()) {
            return;
        }
        if (statistics != null) {
            synchronized (lock) {
                metricView.setValue(statistics.getTickerCount(metricView.tickerType));
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            rocksDB = null;
            statistics = null;
        }
    }

    abstract static class ForStNativeView implements View {
        private boolean closed;

        ForStNativeView() {
            this.closed = false;
        }

        void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    /**
     * A gauge which periodically pulls a ForSt property-based native metric for the specified
     * column family / metric pair.
     *
     * <p><strong>Note</strong>: As the returned property is of type {@code uint64_t} on C++ side
     * the returning value can be negative. Because java does not support unsigned long types, this
     * gauge wraps the result in a {@link BigInteger}.
     */
    class ForStNativePropertyMetricView extends ForStNativeView implements Gauge<BigInteger> {
        private final ForStProperty property;

        private final ColumnFamilyHandle handle;

        private BigInteger bigInteger;

        private ForStNativePropertyMetricView(
                ColumnFamilyHandle handle, @Nonnull ForStProperty property) {
            this.handle = handle;
            this.property = property;
            this.bigInteger = BigInteger.ZERO;
        }

        public void setValue(long value) {
            if (value >= 0L) {
                bigInteger = BigInteger.valueOf(value);
            } else {
                int upper = (int) (value >>> 32);
                int lower = (int) value;

                bigInteger =
                        BigInteger.valueOf(Integer.toUnsignedLong(upper))
                                .shiftLeft(32)
                                .add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
            }
        }

        @Override
        public BigInteger getValue() {
            return bigInteger;
        }

        @Override
        public void update() {
            setProperty(this);
        }
    }

    /** A gauge which periodically pulls a ForSt statistics-based native metric for the database. */
    class ForStNativeStatisticsMetricView extends ForStNativeView implements Gauge<Long> {
        private final TickerType tickerType;
        private long value;

        private ForStNativeStatisticsMetricView(TickerType tickerType) {
            this.tickerType = tickerType;
        }

        @Override
        public Long getValue() {
            return value;
        }

        void setValue(long value) {
            this.value = value;
        }

        @Override
        public void update() {
            setStatistics(this);
        }
    }
}

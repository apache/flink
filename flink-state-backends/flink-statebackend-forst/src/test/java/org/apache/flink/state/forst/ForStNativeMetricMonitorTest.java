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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.traces.SpanBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** validate native metric monitor. */
class ForStNativeMetricMonitorTest {

    private static final String OPERATOR_NAME = "dummy";

    private static final String COLUMN_FAMILY_NAME = "column-family";

    @RegisterExtension public ForStExtension forStExtension = new ForStExtension(true);

    @Test
    void testMetricMonitorLifecycle() throws Throwable {
        // We use a local variable here to manually control the life-cycle.
        // This allows us to verify that metrics do not try to access
        // RocksDB after the monitor was closed.
        ForStExtension localForStExtension = new ForStExtension(true);
        localForStExtension.before();

        SimpleMetricRegistry registry = new SimpleMetricRegistry();
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry,
                        UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                        OPERATOR_NAME);

        ForStNativeMetricOptions options = new ForStNativeMetricOptions();
        // always returns a non-zero
        // value since empty memtables
        // have overhead.
        options.enableSizeAllMemTables();
        options.enableNativeStatistics(ForStNativeMetricOptions.MONITOR_BYTES_WRITTEN);

        ForStNativeMetricMonitor monitor =
                new ForStNativeMetricMonitor(
                        options,
                        group,
                        localForStExtension.getDB(),
                        localForStExtension.getDbOptions().statistics());

        ColumnFamilyHandle handle = localForStExtension.createNewColumnFamily(COLUMN_FAMILY_NAME);
        monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);

        assertThat(registry.propertyMetrics)
                .withFailMessage("Failed to register metrics for column family")
                .hasSize(1);

        // write something to ensure the bytes-written is not zero.
        localForStExtension.getDB().put(new byte[4], new byte[10]);

        for (ForStNativeMetricMonitor.ForStNativePropertyMetricView view :
                registry.propertyMetrics) {
            view.update();
            assertThat(view.getValue())
                    .withFailMessage("Failed to pull metric from ForSt")
                    .isNotEqualTo(BigInteger.ZERO);
            view.setValue(0L);
        }

        for (ForStNativeMetricMonitor.ForStNativeStatisticsMetricView view :
                registry.statisticsMetrics) {
            view.update();
            assertThat(view.getValue()).isNotZero();
            view.setValue(0L);
        }

        // After the monitor is closed no metric should be accessing RocksDB anymore.
        // If they do, then this test will likely fail with a segmentation fault.
        monitor.close();

        localForStExtension.after();

        for (ForStNativeMetricMonitor.ForStNativePropertyMetricView view :
                registry.propertyMetrics) {
            view.update();
            assertThat(view.getValue())
                    .withFailMessage("Failed to release ForSt reference")
                    .isEqualTo(BigInteger.ZERO);
        }

        for (ForStNativeMetricMonitor.ForStNativeStatisticsMetricView view :
                registry.statisticsMetrics) {
            view.update();
            assertThat(view.getValue()).isZero();
        }
    }

    @Test
    void testReturnsUnsigned() throws Throwable {
        ForStExtension localForStExtension = new ForStExtension();
        localForStExtension.before();

        SimpleMetricRegistry registry = new SimpleMetricRegistry();
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry,
                        UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                        OPERATOR_NAME);

        ForStNativeMetricOptions options = new ForStNativeMetricOptions();
        options.enableSizeAllMemTables();

        ForStNativeMetricMonitor monitor =
                new ForStNativeMetricMonitor(
                        options,
                        group,
                        localForStExtension.getDB(),
                        localForStExtension.getDbOptions().statistics());

        ColumnFamilyHandle handle = forStExtension.createNewColumnFamily(COLUMN_FAMILY_NAME);
        monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);
        ForStNativeMetricMonitor.ForStNativePropertyMetricView view =
                registry.propertyMetrics.get(0);

        view.setValue(-1);
        BigInteger result = view.getValue();

        localForStExtension.after();

        assertThat(result.signum())
                .withFailMessage("Failed to interpret ForSt result as an unsigned long")
                .isOne();
    }

    @Test
    void testClosedGaugesDontRead() throws RocksDBException {
        SimpleMetricRegistry registry = new SimpleMetricRegistry();
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry,
                        UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                        OPERATOR_NAME);

        ForStNativeMetricOptions options = new ForStNativeMetricOptions();
        options.enableSizeAllMemTables();
        options.enableNativeStatistics(ForStNativeMetricOptions.MONITOR_BLOCK_CACHE_HIT);

        ForStNativeMetricMonitor monitor =
                new ForStNativeMetricMonitor(
                        options,
                        group,
                        forStExtension.getDB(),
                        forStExtension.getDbOptions().statistics());

        ColumnFamilyHandle handle = forStExtension.createNewColumnFamily(COLUMN_FAMILY_NAME);
        monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);

        forStExtension.getDB().put(new byte[4], new byte[10]);

        for (ForStNativeMetricMonitor.ForStNativePropertyMetricView view :
                registry.propertyMetrics) {
            view.close();
            view.update();
            assertThat(view.getValue())
                    .withFailMessage("Closed gauge still queried ForSt")
                    .isEqualTo(BigInteger.ZERO);
        }

        for (ForStNativeMetricMonitor.ForStNativeStatisticsMetricView view :
                registry.statisticsMetrics) {
            view.close();
            view.update();
            assertThat(view.getValue())
                    .withFailMessage("Closed gauge still queried ForSt")
                    .isZero();
        }
    }

    static class SimpleMetricRegistry implements MetricRegistry {
        List<ForStNativeMetricMonitor.ForStNativePropertyMetricView> propertyMetrics =
                new ArrayList<>();

        List<ForStNativeMetricMonitor.ForStNativeStatisticsMetricView> statisticsMetrics =
                new ArrayList<>();

        @Override
        public char getDelimiter() {
            return 0;
        }

        @Override
        public int getNumberReporters() {
            return 0;
        }

        @Override
        public void addSpan(SpanBuilder spanBuilder) {}

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            if (metric instanceof ForStNativeMetricMonitor.ForStNativePropertyMetricView) {
                propertyMetrics.add(
                        (ForStNativeMetricMonitor.ForStNativePropertyMetricView) metric);
            } else if (metric instanceof ForStNativeMetricMonitor.ForStNativeStatisticsMetricView) {
                statisticsMetrics.add(
                        (ForStNativeMetricMonitor.ForStNativeStatisticsMetricView) metric);
            }
        }

        @Override
        public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {}

        @Override
        public ScopeFormats getScopeFormats() {
            Configuration config = new Configuration();

            config.set(MetricOptions.SCOPE_NAMING_TM, "A");
            config.set(MetricOptions.SCOPE_NAMING_TM_JOB, "B");
            config.set(MetricOptions.SCOPE_NAMING_TASK, "C");
            config.set(MetricOptions.SCOPE_NAMING_OPERATOR, "D");

            return ScopeFormats.fromConfig(config);
        }
    }
}

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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/** validate native metric monitor. */
public class RocksDBNativeMetricMonitorTest {

    private static final String OPERATOR_NAME = "dummy";

    private static final String COLUMN_FAMILY_NAME = "column-family";

    @Rule public RocksDBResource rocksDBResource = new RocksDBResource(true);

    @Test
    public void testMetricMonitorLifecycle() throws Throwable {
        // We use a local variable here to manually control the life-cycle.
        // This allows us to verify that metrics do not try to access
        // RocksDB after the monitor was closed.
        RocksDBResource localRocksDBResource = new RocksDBResource(true);
        localRocksDBResource.before();

        SimpleMetricRegistry registry = new SimpleMetricRegistry();
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry,
                        UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                        OPERATOR_NAME);

        RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
        // always returns a non-zero
        // value since empty memtables
        // have overhead.
        options.enableSizeAllMemTables();
        options.enableNativeStatistics(RocksDBNativeMetricOptions.MONITOR_BYTES_WRITTEN);

        RocksDBNativeMetricMonitor monitor =
                new RocksDBNativeMetricMonitor(
                        options,
                        group,
                        localRocksDBResource.getRocksDB(),
                        localRocksDBResource.getDbOptions().statistics());

        ColumnFamilyHandle handle = localRocksDBResource.createNewColumnFamily(COLUMN_FAMILY_NAME);
        monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);

        Assert.assertEquals(
                "Failed to register metrics for column family", 1, registry.propertyMetrics.size());

        // write something to ensure the bytes-written is not zero.
        localRocksDBResource.getRocksDB().put(new byte[4], new byte[10]);

        for (RocksDBNativeMetricMonitor.RocksDBNativePropertyMetricView view :
                registry.propertyMetrics) {
            view.update();
            Assert.assertNotEquals(
                    "Failed to pull metric from RocksDB", BigInteger.ZERO, view.getValue());
            view.setValue(0L);
        }

        for (RocksDBNativeMetricMonitor.RocksDBNativeStatisticsMetricView view :
                registry.statisticsMetrics) {
            view.update();
            Assert.assertNotEquals(0L, (long) view.getValue());
            view.setValue(0L);
        }

        // After the monitor is closed no metric should be accessing RocksDB anymore.
        // If they do, then this test will likely fail with a segmentation fault.
        monitor.close();

        localRocksDBResource.after();

        for (RocksDBNativeMetricMonitor.RocksDBNativePropertyMetricView view :
                registry.propertyMetrics) {
            view.update();
            Assert.assertEquals(
                    "Failed to release RocksDB reference", BigInteger.ZERO, view.getValue());
        }

        for (RocksDBNativeMetricMonitor.RocksDBNativeStatisticsMetricView view :
                registry.statisticsMetrics) {
            view.update();
            Assert.assertEquals(0L, (long) view.getValue());
        }
    }

    @Test
    public void testReturnsUnsigned() throws Throwable {
        RocksDBResource localRocksDBResource = new RocksDBResource();
        localRocksDBResource.before();

        SimpleMetricRegistry registry = new SimpleMetricRegistry();
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry,
                        UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                        OPERATOR_NAME);

        RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
        options.enableSizeAllMemTables();

        RocksDBNativeMetricMonitor monitor =
                new RocksDBNativeMetricMonitor(
                        options,
                        group,
                        localRocksDBResource.getRocksDB(),
                        localRocksDBResource.getDbOptions().statistics());

        ColumnFamilyHandle handle = rocksDBResource.createNewColumnFamily(COLUMN_FAMILY_NAME);
        monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);
        RocksDBNativeMetricMonitor.RocksDBNativePropertyMetricView view =
                registry.propertyMetrics.get(0);

        view.setValue(-1);
        BigInteger result = view.getValue();

        localRocksDBResource.after();

        Assert.assertEquals(
                "Failed to interpret RocksDB result as an unsigned long", 1, result.signum());
    }

    @Test
    public void testClosedGaugesDontRead() throws RocksDBException {
        SimpleMetricRegistry registry = new SimpleMetricRegistry();
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry,
                        UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                        OPERATOR_NAME);

        RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
        options.enableSizeAllMemTables();
        options.enableNativeStatistics(RocksDBNativeMetricOptions.MONITOR_BLOCK_CACHE_HIT);

        RocksDBNativeMetricMonitor monitor =
                new RocksDBNativeMetricMonitor(
                        options,
                        group,
                        rocksDBResource.getRocksDB(),
                        rocksDBResource.getDbOptions().statistics());

        ColumnFamilyHandle handle = rocksDBResource.createNewColumnFamily(COLUMN_FAMILY_NAME);
        monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);

        rocksDBResource.getRocksDB().put(new byte[4], new byte[10]);

        for (RocksDBNativeMetricMonitor.RocksDBNativePropertyMetricView view :
                registry.propertyMetrics) {
            view.close();
            view.update();
            Assert.assertEquals(
                    "Closed gauge still queried RocksDB", BigInteger.ZERO, view.getValue());
        }

        for (RocksDBNativeMetricMonitor.RocksDBNativeStatisticsMetricView view :
                registry.statisticsMetrics) {
            view.close();
            view.update();
            Assert.assertEquals("Closed gauge still queried RocksDB", 0L, (long) view.getValue());
        }
    }

    static class SimpleMetricRegistry implements MetricRegistry {
        List<RocksDBNativeMetricMonitor.RocksDBNativePropertyMetricView> propertyMetrics =
                new ArrayList<>();

        List<RocksDBNativeMetricMonitor.RocksDBNativeStatisticsMetricView> statisticsMetrics =
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
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            if (metric instanceof RocksDBNativeMetricMonitor.RocksDBNativePropertyMetricView) {
                propertyMetrics.add(
                        (RocksDBNativeMetricMonitor.RocksDBNativePropertyMetricView) metric);
            } else if (metric
                    instanceof RocksDBNativeMetricMonitor.RocksDBNativeStatisticsMetricView) {
                statisticsMetrics.add(
                        (RocksDBNativeMetricMonitor.RocksDBNativeStatisticsMetricView) metric);
            }
        }

        @Override
        public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {}

        @Override
        public ScopeFormats getScopeFormats() {
            Configuration config = new Configuration();

            config.setString(MetricOptions.SCOPE_NAMING_TM, "A");
            config.setString(MetricOptions.SCOPE_NAMING_TM_JOB, "B");
            config.setString(MetricOptions.SCOPE_NAMING_TASK, "C");
            config.setString(MetricOptions.SCOPE_NAMING_OPERATOR, "D");

            return ScopeFormats.fromConfig(config);
        }
    }
}

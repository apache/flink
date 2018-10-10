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

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.util.ResourceGuard;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;

/**
 * validate native metric monitor.
 */
public class RocksDBNativeMetricMonitorTest {

	private static final String COLUMN_FAMILY_NAME = "column-family";

	private static final String PARENT_GROUP_NAME = "parent";

	@Rule
	public RocksDBResource rocksDBResource = new RocksDBResource();

	@Test
	public void testMetricMonitorLifecycle() throws IOException {
		ResourceGuard guard = new ResourceGuard();
		SimpleMetricRegistry registry = new SimpleMetricRegistry();
		AbstractMetricGroup<?> dummyGroup = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
		MetricGroup metricGroup = new GenericMetricGroup(registry, dummyGroup, PARENT_GROUP_NAME);

		RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
		options.enableSizeAllMemTables();

		RocksDBNativeMetricMonitor monitor = new RocksDBNativeMetricMonitor(
			rocksDBResource.getRocksDB(),
			guard,
			options,
			metricGroup
		);

		Assert.assertEquals("Failed to acquire lease", 1, guard.getLeaseCount());

		ColumnFamilyHandle handle = rocksDBResource.createNewColumnFamily(COLUMN_FAMILY_NAME);
		monitor.registerColumnFamily(COLUMN_FAMILY_NAME, handle);

		Assert.assertEquals("Failed to register metrics for column family", 1, registry.metrics.size());

		monitor.close();

		Assert.assertEquals("Failed to release lease", 0, guard.getLeaseCount());

		RocksDBNativeMetricMonitor.RocksDBNativeMetricView view =
			(RocksDBNativeMetricMonitor.RocksDBNativeMetricView) registry.metrics.get(0);

		Assert.assertFalse("Failed to close native metric view", view.isOpen());
	}

	static class SimpleMetricRegistry implements MetricRegistry {
		ArrayList<Metric> metrics = new ArrayList<>();

		@Override
		public char getDelimiter() {
			return 0;
		}

		@Override
		public char getDelimiter(int index) {
			return 0;
		}

		@Override
		public int getNumberReporters() {
			return 0;
		}

		@Override
		public void register(Metric metric, String metricName, AbstractMetricGroup group) {
			metrics.add(metric);
		}

		@Override
		public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {

		}

		@Override
		public ScopeFormats getScopeFormats() {
			return null;
		}

		@Nullable
		@Override
		public String getMetricQueryServicePath() {
			return null;
		}
	}
}

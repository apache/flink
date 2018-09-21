/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.ResourceGuard;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that native metrics can be pulled from RocksDB.
 */
public class RocksDBNativeMetricTest {

	private static final String COLUMN_FAMILY_NAME = "test-cf";

	private static final String PROPERTY = "property";

	@Rule public RocksDBResource rocksDBResource = new RocksDBResource();

	@Test
	public void testMessageCollector() throws IOException {
		ConcurrentLinkedQueue<Tuple2<String, ColumnFamilyHandle>> messageQueue = new ConcurrentLinkedQueue<>();

		RocksDBNativeMetricMonitor.RocksDBNativeMetricTask task = new RocksDBNativeMetricMonitor.RocksDBNativeMetricTask(
			mock(RocksDB.class),
			new ResourceGuard(),
			new ArrayList<>(),
			mock(MetricGroup.class),
			messageQueue
		);

		messageQueue.add(Tuple2.of("", mock(ColumnFamilyHandle.class)));
		task.collectNewMessages();

		Assert.assertEquals("failed to removed pending messages from queue", 0, messageQueue.size());
		Assert.assertEquals("failed to add message to kv state", 1, task.numberOfColumnFamilies());
	}

	@Test
	public void testGaugeRegistration() throws IOException {
		MetricGroup metricGroup = mock(MetricGroup.class);
		MetricGroup innerGroup = mock(MetricGroup.class);

		when(metricGroup.addGroup(COLUMN_FAMILY_NAME)).thenReturn(innerGroup);

		RocksDBNativeMetricMonitor.RocksDBNativeMetricTask task = new RocksDBNativeMetricMonitor.RocksDBNativeMetricTask(
			mock(RocksDB.class),
			new ResourceGuard(),
			Collections.singleton(PROPERTY),
			metricGroup,
			new ConcurrentLinkedQueue<>()
		);

		Map<String, RocksDBNativeMetricMonitor.NativePropertyGauge> gauges = task.getGaugesForColumnFamily(COLUMN_FAMILY_NAME);

		Assert.assertEquals("failed to register gauges for column family", 1, gauges.size());
		Assert.assertNotNull("failed to register correct gauge for column family", gauges.get(PROPERTY));

		verify(innerGroup, times(1)).gauge(eq(PROPERTY), any());

		Map<String, RocksDBNativeMetricMonitor.NativePropertyGauge> recallGauges = task.getGaugesForColumnFamily(COLUMN_FAMILY_NAME);

		Assert.assertSame("failed to return preregistered gauge", gauges, recallGauges);
	}

	@Test
	public void testPropertyRead() throws IOException, RocksDBException {
		MetricGroup metricGroup = mock(MetricGroup.class);
		MetricGroup innerGroup = mock(MetricGroup.class);

		when(metricGroup.addGroup(COLUMN_FAMILY_NAME)).thenReturn(innerGroup);

		RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();

		//this property is guaranteed to return a non-zero value
		//since empty mem-tables still have overhead
		options.enableSizeAllMemTables();
		options.enableEstimateNumKeys();

		ConcurrentLinkedQueue<Tuple2<String, ColumnFamilyHandle>>  messageQueue = new ConcurrentLinkedQueue<>();

		RocksDBNativeMetricMonitor.RocksDBNativeMetricTask task = new RocksDBNativeMetricMonitor.RocksDBNativeMetricTask(
			rocksDBResource.getRocksDB(),
			new ResourceGuard(),
			options.getProperties(),
			metricGroup,
			messageQueue
		);

		ColumnFamilyHandle handle = rocksDBResource.createNewColumnFamily(COLUMN_FAMILY_NAME);
		messageQueue.add(Tuple2.of(COLUMN_FAMILY_NAME, handle));

		byte[] data = {1, 1, 1, 1};
		rocksDBResource.getRocksDB().put(handle, data, data);

		task.run();

		Assert.assertEquals("failed to register property", 0, messageQueue.size());

		Map<String, RocksDBNativeMetricMonitor.NativePropertyGauge> gauges = task.getGaugesForColumnFamily(COLUMN_FAMILY_NAME);
		Assert.assertEquals("failed to register gauges for column family", 2,  gauges.size());

		gauges.forEach((property, gauge) -> {
			Assert.assertNotEquals(String.format("failed to pull metrics for property %s", property), new Long(0), gauge.getValue());
		});
	}

	@Test
	public void testResourceLeaseLifecycle() throws IOException {
		ResourceGuard guard = new ResourceGuard();

		RocksDBNativeMetricMonitor monitor = new RocksDBNativeMetricMonitor(
			mock(RocksDB.class),
			guard,
			new RocksDBNativeMetricOptions(),
			mock(MetricGroup.class)
		);

		Assert.assertEquals("failed to acquire lease", 1, guard.getLeaseCount());

		monitor.close();

		Assert.assertEquals("failed to release lease on close", 0, guard.getLeaseCount());
	}
}

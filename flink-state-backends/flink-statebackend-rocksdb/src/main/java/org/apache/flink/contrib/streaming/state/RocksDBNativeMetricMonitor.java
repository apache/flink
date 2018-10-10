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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;

/**
 * A monitor which pull {{@link RocksDB}} native metrics
 * and forwards them to Flink's metric group. All metrics are
 * unsigned longs and are reported at the column family level.
 */
@Internal
public class RocksDBNativeMetricMonitor implements Closeable {

	private final CloseableRegistry registeredGauges;

	private final RocksDB db;

	private final ResourceGuard.Lease lease;

	private final RocksDBNativeMetricOptions options;

	private final MetricGroup metricGroup;

	RocksDBNativeMetricMonitor(
		@Nonnull RocksDB db,
		@Nonnull ResourceGuard guard,
		@Nonnull RocksDBNativeMetricOptions options,
		@Nonnull MetricGroup metricGroup
	) throws IOException {
		this.db = db;
		this.lease = guard.acquireResource();
		this.options = options;
		this.metricGroup = metricGroup;

		this.registeredGauges = new CloseableRegistry();
	}

	/**
	 * Register gauges to pull native metrics for the column family.
	 * @param columnFamilyName group name for the new gauges
	 * @param handle native handle to the column family
	 */
	void registerColumnFamily(String columnFamilyName, ColumnFamilyHandle handle) {
		try {
			MetricGroup group = metricGroup.addGroup(columnFamilyName);

			for (String property : options.getProperties()) {
				RocksDBNativeMetricView gauge = new RocksDBNativeMetricView(
					property,
					handle,
					db
				);

				group.gauge(property, gauge);
				registeredGauges.registerCloseable(gauge);
			}
		} catch (IOException e) {
			throw new FlinkRuntimeException("Unable to register native metrics with RocksDB", e);
		}
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(registeredGauges);
		IOUtils.closeQuietly(lease);
	}

	static class RocksDBNativeMetricView implements Gauge<String>, View, Closeable {
		private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeMetricView.class);

		private final String property;

		private final ColumnFamilyHandle handle;

		private final RocksDB db;

		private volatile boolean open;

		private long value;

		private RocksDBNativeMetricView(
			@Nonnull String property,
			@Nonnull ColumnFamilyHandle handle,
			@Nonnull RocksDB db
		) {
			this.property = property;
			this.handle = handle;
			this.db = db;
			this.open = true;
		}

		@Override
		public String getValue() {
			return Long.toUnsignedString(value);
		}

		@Override
		public void update() {
			if (open) {
				try {
					value = db.getLongProperty(handle, property);
				} catch (RocksDBException e) {
					LOG.warn("Failed to read native metric %s from RocksDB", property, e);
				}
			}
		}

		public boolean isOpen() {
			return open;
		}

		@Override
		public void close() {
			open = false;
		}
	}

}


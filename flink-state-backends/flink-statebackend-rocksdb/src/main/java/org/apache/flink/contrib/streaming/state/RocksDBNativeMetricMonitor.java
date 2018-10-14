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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;

/**
 * A monitor which pull {{@link RocksDB}} native metrics
 * and forwards them to Flink's metric group. All metrics are
 * unsigned longs and are reported at the column family level.
 */
@Internal
public class RocksDBNativeMetricMonitor implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeMetricMonitor.class);

	private final RocksDBNativeMetricOptions options;

	private final OperatorMetricGroup metricGroup;

	private final Object lock;

	private RocksDB rocksDB;

	RocksDBNativeMetricMonitor(
		@Nonnull RocksDB rocksDB,
		@Nonnull RocksDBNativeMetricOptions options,
		@Nonnull OperatorMetricGroup metricGroup
	) {
		this.options = options;
		this.metricGroup = metricGroup;
		this.rocksDB = rocksDB;

		this.lock = new Object();
	}

	/**
	 * Register gauges to pull native metrics for the column family.
	 * @param columnFamilyName group name for the new gauges
	 * @param handle native handle to the column family
	 */
	void registerColumnFamily(String columnFamilyName, ColumnFamilyHandle handle) {
		MetricGroup group = metricGroup.addGroup(columnFamilyName);

		for (String property : options.getProperties()) {
			RocksDBNativeMetricView gauge = new RocksDBNativeMetricView(handle, property);
			group.gauge(property, gauge);
		}
	}

	/**
	 * Updates the value of metricView if the reference is still valid.
	 */
	private void setProperty(ColumnFamilyHandle handle, String property, RocksDBNativeMetricView metricView) {
		try {
			synchronized (lock) {
				if (rocksDB != null) {
					long value = rocksDB.getLongProperty(handle, property);
					metricView.setValue(value);
				}
			}
		} catch (RocksDBException e) {
			LOG.warn("Failed to read native metric %s from RocksDB", property, e);
		}
	}

	@Override
	public void close() {
		synchronized (lock) {
			this.rocksDB = null;
		}
	}

	class RocksDBNativeMetricView implements Gauge<String>, View {
		private final String property;

		private final ColumnFamilyHandle handle;


		private long value;

		private RocksDBNativeMetricView(
			@Nonnull ColumnFamilyHandle handle,
			@Nonnull String property
		) {
			this.handle = handle;
			this.property = property;
		}

		public void setValue(long value) {
			this.value = value;
		}

		@Override
		public String getValue() {
			return Long.toUnsignedString(value);
		}

		@Override
		public void update() {
			setProperty(handle, property, this);
		}
	}
}


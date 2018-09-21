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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A monitor which pull {{@link RocksDB}} native metrics
 * and forwards them to Flink's metric group. All metrics are
 * unsigned longs and are reported at the column family level.
 */
@Internal
public class RocksDBNativeMetricMonitor implements Closeable {

	private final TimerTask task;

	private final Timer timer;

	private final ConcurrentLinkedQueue<Tuple2<String, ColumnFamilyHandle>> messageQueue;

	RocksDBNativeMetricMonitor(
		@Nonnull RocksDB db,
		@Nonnull ResourceGuard rocksDBResourceGuard,
		@Nonnull RocksDBNativeMetricOptions rocksDBNativeProperyOptions,
		@Nonnull MetricGroup metricGroup
	) throws IOException {
		this.messageQueue = new ConcurrentLinkedQueue<>();

		this.task = new RocksDBNativeMetricTask(
			db,
			rocksDBResourceGuard,
			rocksDBNativeProperyOptions.getProperties(),
			metricGroup,
			messageQueue
		);

		long frequency = rocksDBNativeProperyOptions.getFrequency();
		this.timer = new Timer();
		this.timer.schedule(task, frequency, frequency);
	}

	void watch(String columnFamilyName, ColumnFamilyHandle handle) {
		this.messageQueue.add(Tuple2.of(columnFamilyName, handle));
	}

	@Override
	public void close() {
		this.task.cancel();
		this.timer.cancel();
	}

	static class RocksDBNativeMetricTask extends TimerTask {
		private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeMetricTask.class);

		private final RocksDB db;

		private final ResourceGuard.Lease lease;

		private final Map<String, ColumnFamilyHandle> kvStateInformation;

		private final MetricGroup metricGroup;

		private final Map<String, Map<String, NativePropertyGauge>> cachedGaugesByColumnFamily;

		private final Collection<String> properties;

		private final ConcurrentLinkedQueue<Tuple2<String, ColumnFamilyHandle>> messageQueue;

		RocksDBNativeMetricTask(
			@Nonnull RocksDB db,
			@Nonnull ResourceGuard rocksDBResourceGuard,
			@Nonnull Collection<String> properties,
			@Nonnull MetricGroup metricGroup,
			@Nonnull ConcurrentLinkedQueue<Tuple2<String, ColumnFamilyHandle>> messageQueue
		) throws IOException {
			this.db = db;
			this.lease = rocksDBResourceGuard.acquireResource();
			this.kvStateInformation = new HashMap<>();
			this.cachedGaugesByColumnFamily = new HashMap<>();
			this.properties = properties;
			this.metricGroup = metricGroup;
			this.messageQueue = messageQueue;
		}

		@Override
		public void run() {
			collectNewMessages();

			kvStateInformation.forEach((columnFamilyName, columnFamilyHandle) -> {
				getGaugesForColumnFamily(columnFamilyName).forEach((property, gauge) -> {
					try {
						long value = db.getLongProperty(columnFamilyHandle, property);
						gauge.setValue(value);
					} catch (RocksDBException e) {
						LOG.warn("Failed to read native metric %s for column family %s from RocksDB", property, columnFamilyName, e);
					}
				});
			});
		}

		void collectNewMessages() {
			Iterator<Tuple2<String, ColumnFamilyHandle>> iterator = messageQueue.iterator();
			while (iterator.hasNext()) {
				Tuple2<String, ColumnFamilyHandle> message = iterator.next();
				iterator.remove();
				kvStateInformation.put(message.f0, message.f1);
			}
		}

		int numberOfColumnFamilies() {
			return kvStateInformation.size();
		}

		Map<String, NativePropertyGauge> getGaugesForColumnFamily(String columnFamilyName) {
			return cachedGaugesByColumnFamily.computeIfAbsent(columnFamilyName, key -> {
				MetricGroup group = metricGroup.addGroup(key);
				Map<String, NativePropertyGauge> newGauges = new HashMap<>(properties.size());

				for (String property : properties) {
					NativePropertyGauge gauge = new NativePropertyGauge();
					group.gauge(property, gauge);
					newGauges.put(property, gauge);
				}

				return newGauges;
			});
		}

		@Override
		public boolean cancel() {
			IOUtils.closeQuietly(lease);
			return super.cancel();
		}
	}

	/**
	 * A gauge which treats the value as an unsigned long.
	 */
	static class NativePropertyGauge implements Gauge<String> {
		private volatile long value;

		public void setValue(long value) {
			this.value = value;
		}

		public long getLongValue() {
			return this.value;
		}

		@Override
		public String getValue() {
			return Long.toUnsignedString(value);
		}
	}
}


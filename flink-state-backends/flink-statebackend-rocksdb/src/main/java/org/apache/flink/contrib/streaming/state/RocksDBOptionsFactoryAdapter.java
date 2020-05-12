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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ReadableConfig;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * A conversion from {@link OptionsFactory} to {@link RocksDBOptionsFactory}.
 */
@SuppressWarnings("deprecation")
final class RocksDBOptionsFactoryAdapter implements ConfigurableRocksDBOptionsFactory {

	private static final long serialVersionUID = 1L;

	private final OptionsFactory optionsFactory;

	RocksDBOptionsFactoryAdapter(OptionsFactory optionsFactory) {
		this.optionsFactory = optionsFactory;
	}

	@Override
	public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
		return optionsFactory.createDBOptions(currentOptions);
	}

	@Override
	public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
		return optionsFactory.createColumnOptions(currentOptions);
	}

	@Override
	public RocksDBNativeMetricOptions createNativeMetricsOptions(RocksDBNativeMetricOptions nativeMetricOptions) {
		return optionsFactory.createNativeMetricsOptions(nativeMetricOptions);
	}

	@Override
	public RocksDBOptionsFactory configure(ReadableConfig configuration) {
		if (optionsFactory instanceof ConfigurableOptionsFactory) {
			final OptionsFactory reconfigured = ((ConfigurableOptionsFactory) optionsFactory).configure(configuration);
			return reconfigured == optionsFactory ? this : new RocksDBOptionsFactoryAdapter(reconfigured);
		}

		return this;
	}

	@Nullable
	public static OptionsFactory unwrapIfAdapter(RocksDBOptionsFactory factory) {
		return factory instanceof RocksDBOptionsFactoryAdapter
				? ((RocksDBOptionsFactoryAdapter) factory).optionsFactory
				: factory;
	}
}

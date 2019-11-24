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

import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * The container of RocksDB option factory and predefined options.
 * <p/>
 * This should be the only entrance for {@link RocksDBStateBackend} to get RocksDB options,
 * and should be properly (and necessarily) closed to prevent resource leak.
 */
public class RocksDBOptionsContainer implements AutoCloseable, Serializable {

	/** The pre-configured option settings. */
	private PredefinedOptions predefinedOptions;
	/** The options factory to create the RocksDB options. */
	private OptionsFactory optionsFactory;

	private final ArrayList<DBOptions> dbOptions;
	private final ArrayList<ColumnFamilyOptions> columnFamilyOptions;

	public RocksDBOptionsContainer() {
		dbOptions = new ArrayList<>();
		columnFamilyOptions = new ArrayList<>();
	}

	/**
	 * Gets the RocksDB {@link DBOptions} to be used for RocksDB instances.
	 */
	DBOptions getDbOptions() {
		// initial options from pre-defined profile
		DBOptions opt = checkAndGetPredefinedOptions().createDBOptions();

		// add user-defined options factory, if specified
		if (optionsFactory != null) {
			opt = optionsFactory.createDBOptions(opt);
		}

		// add necessary default options
		opt = opt.setCreateIfMissing(true);

		// record the initiated options for resource guard
		dbOptions.add(opt);

		return opt;
	}

	/**
	 * Gets the RocksDB {@link ColumnFamilyOptions} to be used for all RocksDB instances.
	 */
	public ColumnFamilyOptions getColumnOptions() {
		// initial options from pre-defined profile
		ColumnFamilyOptions opt = checkAndGetPredefinedOptions().createColumnOptions();

		// add user-defined options, if specified
		if (optionsFactory != null) {
			opt = optionsFactory.createColumnOptions(opt);
		}

		// record the initiated options for resource guard
		columnFamilyOptions.add(opt);

		return opt;
	}

	PredefinedOptions getPredefinedOptions() {
		return predefinedOptions;
	}

	PredefinedOptions checkAndGetPredefinedOptions() {
		if (predefinedOptions == null) {
			predefinedOptions = PredefinedOptions.DEFAULT;
		}
		return predefinedOptions;
	}

	OptionsFactory getOptionsFactory() {
		return optionsFactory;
	}

	void setPredefinedOptions(@Nonnull PredefinedOptions predefinedOptions) {
		this.predefinedOptions = predefinedOptions;
	}

	void setOptionsFactory(@Nonnull OptionsFactory optionsFactory) {
		this.optionsFactory = optionsFactory;
	}

	@Override
	public void close() throws Exception {
		if (optionsFactory != null) {
			optionsFactory.close();
		}
		if (predefinedOptions != null) {
			predefinedOptions.close();
		}
		dbOptions.forEach(IOUtils::closeQuietly);
		columnFamilyOptions.forEach(IOUtils::closeQuietly);
		// we need to consider for the container reuse after restore processing
		dbOptions.clear();
		columnFamilyOptions.clear();
	}
}

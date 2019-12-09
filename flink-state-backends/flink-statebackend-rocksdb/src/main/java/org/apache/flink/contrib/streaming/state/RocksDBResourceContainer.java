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

import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import javax.annotation.Nonnull;

import java.util.ArrayList;

/**
 * The container for RocksDB resources, including predefined options, option factory and
 * shared resource among instances.
 * <p/>
 * This should be the only entrance for {@link RocksDBStateBackend} to get RocksDB options,
 * and should be properly (and necessarily) closed to prevent resource leak.
 */
public class RocksDBResourceContainer implements AutoCloseable {

	/** The pre-configured option settings. */
	private PredefinedOptions predefinedOptions;
	/** The options factory to create the RocksDB options. */
	private OptionsFactory optionsFactory;
	/** The shared resource among RocksDB instances. */
	private OpaqueMemoryResource<RocksDBSharedResources> sharedResources;

	private final ArrayList<AutoCloseable> handlesToClose;

	public RocksDBResourceContainer() {
		handlesToClose = new ArrayList<>();
	}

	/**
	 * Gets the RocksDB {@link DBOptions} to be used for RocksDB instances.
	 */
	DBOptions getDbOptions() {
		// initial options from pre-defined profile
		DBOptions opt = checkAndGetPredefinedOptions().createDBOptions(handlesToClose);

		// add user-defined options factory, if specified
		if (optionsFactory != null) {
			opt = optionsFactory.createDBOptions(opt, handlesToClose);
		}

		// add necessary default options
		opt = opt.setCreateIfMissing(true);

		return opt;
	}

	/**
	 * Gets the RocksDB {@link ColumnFamilyOptions} to be used for all RocksDB instances.
	 */
	public ColumnFamilyOptions getColumnOptions() {
		// initial options from pre-defined profile
		ColumnFamilyOptions opt = checkAndGetPredefinedOptions().createColumnOptions(handlesToClose);

		// add user-defined options, if specified
		if (optionsFactory != null) {
			opt = optionsFactory.createColumnOptions(opt, handlesToClose);
		}

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

	void setSharedResources(OpaqueMemoryResource<RocksDBSharedResources> sharedResources) {
		this.sharedResources = sharedResources;
	}

	@Override
	public void close() throws Exception {
		handlesToClose.forEach(IOUtils::closeQuietly);
		handlesToClose.clear();

		if (sharedResources != null) {
			sharedResources.close();
		}
	}
}

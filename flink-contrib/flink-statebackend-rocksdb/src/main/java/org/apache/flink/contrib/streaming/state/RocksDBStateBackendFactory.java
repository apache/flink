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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackendFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * A factory that creates an {@link org.apache.flink.contrib.streaming.state.RocksDBStateBackend}
 * from a configuration.
 */
public class RocksDBStateBackendFactory implements StateBackendFactory<RocksDBStateBackend> {

	protected static final Logger LOG = LoggerFactory.getLogger(RocksDBStateBackendFactory.class);

	private static final long serialVersionUID = 4906988360901930371L;

	/** The key under which the config stores the directory where checkpoints should be stored. */
	public static final String CHECKPOINT_DIRECTORY_URI_CONF_KEY = "state.backend.fs.checkpointdir";
	/** The key under which the config stores the directory where RocksDB should be stored. */
	public static final String ROCKSDB_CHECKPOINT_DIRECTORY_URI_CONF_KEY = "state.backend.rocksdb.checkpointdir";

	/** The key defines the pattern in checkpoint uri where entropy value should be injected or substituted. */
	public static final String CHECKPOINT_DIRECTORY_ENTROPY_KEY_CONF_KEY = "state.backend.fs.checkpointdir.injectEntropy.key";

	@Override
	public RocksDBStateBackend createFromConfig(Configuration config)
			throws IllegalConfigurationException, IOException {

		final String checkpointDirURI = config.getString(CHECKPOINT_DIRECTORY_URI_CONF_KEY, null);
		final String rocksdbLocalPath = config.getString(ROCKSDB_CHECKPOINT_DIRECTORY_URI_CONF_KEY, null);
		final String entropyInjectionKey = config.getString(CHECKPOINT_DIRECTORY_ENTROPY_KEY_CONF_KEY, "__ENTROPY_KEY__");

		if (checkpointDirURI == null) {
			throw new IllegalConfigurationException(
				"Cannot create the RocksDB state backend: The configuration does not specify the " +
				"checkpoint directory '" + CHECKPOINT_DIRECTORY_URI_CONF_KEY + '\'');
		}

		try {
			Path path = new Path(checkpointDirURI);
			RocksDBStateBackend backend = new RocksDBStateBackend(path.toUri(),
				true, entropyInjectionKey);
			if (rocksdbLocalPath != null) {
				String[] directories = rocksdbLocalPath.split(",|" + File.pathSeparator);
				backend.setDbStoragePaths(directories);
			}
			LOG.info("State backend is set to RocksDB (configured DB storage paths {}, checkpoints to filesystem {} ) ",
					backend.getDbStoragePaths(), path);

			return backend;
		}
		catch (IllegalArgumentException e) {
			throw new IllegalConfigurationException(
					"Cannot initialize RocksDB State Backend with URI '" + checkpointDirURI + '.', e);
		}
	}
}

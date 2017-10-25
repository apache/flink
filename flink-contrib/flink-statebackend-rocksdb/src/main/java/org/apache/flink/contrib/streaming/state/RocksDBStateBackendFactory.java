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

import org.apache.flink.configuration.CheckpointingOptions;
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

	@Override
	public RocksDBStateBackend createFromConfig(Configuration config)
			throws IllegalConfigurationException, IOException {

		final String checkpointDirURI = config.getString(CheckpointingOptions.CHECKPOINTS_DIRECTORY);
		final String rocksdbLocalPaths = config.getString(CheckpointingOptions.ROCKSDB_LOCAL_DIRECTORIES);
		final boolean incrementalCheckpoints = config.getBoolean(CheckpointingOptions.ROCKSDB_INCREMENTAL_CHECKPOINTS);

		if (checkpointDirURI == null) {
			throw new IllegalConfigurationException(
				"Cannot create the RocksDB state backend: The configuration does not specify the " +
				"checkpoint directory '" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + '\'');
		}

		try {
			final Path path = new Path(checkpointDirURI);
			final RocksDBStateBackend backend = new RocksDBStateBackend(path.toUri(), incrementalCheckpoints);

			if (rocksdbLocalPaths != null) {
				String[] directories = rocksdbLocalPaths.split(",|" + File.pathSeparator);
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

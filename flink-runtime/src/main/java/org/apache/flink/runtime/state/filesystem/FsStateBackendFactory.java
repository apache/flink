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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackendFactory;

import java.net.URI;

/**
 * A factory that creates an {@link FsStateBackend} from a configuration.
 */
@PublicEvolving
public class FsStateBackendFactory implements StateBackendFactory<FsStateBackend> {

	/** Config key for the maximum state size that is stored with the metadata */
	public static final ConfigOption<Integer> MEMORY_THRESHOLD_CONF = ConfigOptions
			.key("state.backend.fs.memory-threshold")
			.defaultValue(FsStateBackend.DEFAULT_FILE_STATE_THRESHOLD);

	// ------------------------------------------------------------------------

	@Override
	public FsStateBackend createFromConfig(Configuration config) throws IllegalConfigurationException {
		final String checkpointDir = config.getString(AbstractFileStateBackend.CHECKPOINT_PATH);
		final String savepointDir = config.getString(AbstractFileStateBackend.SAVEPOINT_PATH);
		final int memoryThreshold = config.getInteger(MEMORY_THRESHOLD_CONF);

		if (checkpointDir == null) {
			throw new IllegalConfigurationException(
					"Cannot create the file system state backend: The configuration does not specify the " +
							"checkpoint directory '" + AbstractFileStateBackend.CHECKPOINT_PATH.key() + '\'');
		}

		try {
			URI checkpointDirUri = new Path(checkpointDir).toUri();
			URI savepointDirUri = savepointDir == null ? null : new Path(savepointDir).toUri();
			return new FsStateBackend(checkpointDirUri, savepointDirUri, memoryThreshold);
		}
		catch (IllegalArgumentException e) {
			throw new IllegalConfigurationException("Invalid configuration for the state backend", e);
		}
	}
}

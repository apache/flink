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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.state.StateBackendFactory;

/**
 * A factory that creates an {@link FsStateBackend} from a configuration.
 */
@PublicEvolving
public class FsStateBackendFactory implements StateBackendFactory<FsStateBackend> {

	@Override
	public FsStateBackend createFromConfig(Configuration config, ClassLoader classLoader) throws IllegalConfigurationException {
		// we need to explicitly read the checkpoint directory here, because that
		// is a required constructor parameter
		final String checkpointDir = config.getString(CheckpointingOptions.CHECKPOINTS_DIRECTORY);
		if (checkpointDir == null) {
			throw new IllegalConfigurationException(
					"Cannot create the file system state backend: The configuration does not specify the " +
							"checkpoint directory '" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + '\'');
		}

		try {
			return new FsStateBackend(checkpointDir).configure(config, classLoader);
		}
		catch (IllegalArgumentException e) {
			throw new IllegalConfigurationException("Invalid configuration for the state backend", e);
		}
	}
}

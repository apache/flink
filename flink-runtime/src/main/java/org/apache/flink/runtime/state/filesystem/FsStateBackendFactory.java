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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackendFactory;

/**
 * A factory that creates an {@link org.apache.flink.runtime.state.filesystem.FsStateBackend}
 * from a configuration.
 */
public class FsStateBackendFactory implements StateBackendFactory<FsStateBackend> {
	
	/** The key under which the config stores the directory where checkpoints should be stored */
	public static final String CHECKPOINT_DIRECTORY_URI_CONF_KEY = "state.backend.fs.checkpointdir";
	
	
	@Override
	public FsStateBackend createFromConfig(Configuration config) throws Exception {
		String checkpointDirURI = config.getString(CHECKPOINT_DIRECTORY_URI_CONF_KEY, null);

		if (checkpointDirURI == null) {
			throw new IllegalConfigurationException(
					"Cannot create the file system state backend: The configuration does not specify the " +
							"checkpoint directory '" + CHECKPOINT_DIRECTORY_URI_CONF_KEY + '\'');
		}
		
		try {
			Path path = new Path(checkpointDirURI);
			return new FsStateBackend(path);
		}
		catch (IllegalArgumentException e) {
			throw new Exception("Cannot initialize File System State Backend with URI '"
					+ checkpointDirURI + '.', e);
		}
		
	}
}

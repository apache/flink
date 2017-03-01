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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.filesystem.AbstractFileStateBackend;

import java.util.UUID;

/**
 * A factory that creates an {@link MemoryStateBackend} from a configuration.
 */
@PublicEvolving
public class MemoryStateBackendFactory implements StateBackendFactory<MemoryStateBackend> {

	@Override
	public MemoryStateBackend createFromConfig(Configuration config) {
		String checkpointDir = config.getString(AbstractFileStateBackend.CHECKPOINT_PATH);
		String savepointDir = config.getString(AbstractFileStateBackend.SAVEPOINT_PATH);

		// to keep supporting the old behavior where Mem Backend + HA mode = checkpoints in HA store
		// we add the HA persistence dir as the checkpoint directory if none other is set
		if (checkpointDir == null && HighAvailabilityMode.isHighAvailabilityModeActivated(config)) {
			String haStoragePath = config.getString(HighAvailabilityOptions.HA_STORAGE_PATH);

			if (haStoragePath != null) {
				try {
					Path checkpointDirPath = new Path(haStoragePath, UUID.randomUUID().toString());
					if (checkpointDirPath.toUri().getScheme() == null) {
						checkpointDirPath = checkpointDirPath.makeQualified(checkpointDirPath.getFileSystem());
					}
					checkpointDir = checkpointDirPath.toString();
				} catch (Exception ignored) {}
			}
		}

		return new MemoryStateBackend(checkpointDir, savepointDir);
	}
}

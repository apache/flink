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

package org.apache.flink.runtime.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;

import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/**
 * This class encapsulates the completed configuration for local recovery, i.e. the root
 * directories into which all file-based snapshots can be written and the general mode for the local recover feature.
 */
public class LocalRecoveryConfig {

	/**
	 * Enum over modes of local recovery:
	 * <p><ul>
	 * <li>DISABLED: disables local recovery.
	 * <li>ENABLE_FILE_BASED: enables local recovery in a variant that is based on local files.
	 * </ul>
	 */
	public enum LocalRecoveryMode {
		DISABLED,
		ENABLE_FILE_BASED;

		/**
		 * Extracts the {@link LocalRecoveryMode} from the given configuration. Defaults to LocalRecoveryMode.DISABLED
		 * if no configuration value is specified or parsing the value resulted in an exception.
		 *
		 * @param configuration the configuration that specifies the value for the local recovery mode.
		 * @return the local recovery mode as found in the config, or LocalRecoveryMode.DISABLED if no mode was
		 * configured or the specified mode could not be parsed.
		 */
		@Nonnull
		public static LocalRecoveryMode fromConfig(@Nonnull Configuration configuration) {
			String localRecoveryConfString = configuration.getString(CheckpointingOptions.LOCAL_RECOVERY);
			try {
				return LocalRecoveryConfig.LocalRecoveryMode.valueOf(localRecoveryConfString);
			} catch (IllegalArgumentException ex) {
				LoggerFactory.getLogger(LocalRecoveryConfig.class).warn(
					"Exception while parsing configuration of local recovery mode. Local recovery will be disabled.",
					ex);
				return LocalRecoveryConfig.LocalRecoveryMode.DISABLED;
			}
		}
	}

	/** The local recovery mode. */
	@Nonnull
	private final LocalRecoveryMode localRecoveryMode;

	/** Encapsulates the root directories and the subtask-specific path. */
	@Nonnull
	private final LocalRecoveryDirectoryProvider localStateDirectories;

	public LocalRecoveryConfig(
		@Nonnull LocalRecoveryMode localRecoveryMode,
		@Nonnull LocalRecoveryDirectoryProvider directoryProvider) {
		this.localRecoveryMode = localRecoveryMode;
		this.localStateDirectories = directoryProvider;
	}

	@Nonnull
	public LocalRecoveryMode getLocalRecoveryMode() {
		return localRecoveryMode;
	}

	@Nonnull
	public LocalRecoveryDirectoryProvider getLocalStateDirectoryProvider() {
		return localStateDirectories;
	}

	@Override
	public String toString() {
		return "LocalRecoveryConfig{" +
			"localRecoveryMode=" + localRecoveryMode +
			", localStateDirectories=" + localStateDirectories +
			'}';
	}
}

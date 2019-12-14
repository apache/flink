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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.ConfigConstants;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Optional;

/**
 * Utility class for {@link org.apache.flink.runtime.entrypoint.ClusterEntrypoint}.
 */
public final class ClusterEntrypointUtils {

	private ClusterEntrypointUtils() {
		throw new UnsupportedOperationException("This class should not be instantiated.");
	}

	/**
	 * Tries to find the user library directory.
	 *
	 * @return the user library directory if it exits, returns {@link Optional#empty()} if there is none
	 */
	public static Optional<File> tryFindUserLibDirectory() {
		final File flinkHomeDirectory = deriveFlinkHomeDirectoryFromLibDirectory();
		final File usrLibDirectory = new File(flinkHomeDirectory, ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);

		if (!usrLibDirectory.isDirectory()) {
			return Optional.empty();
		}
		return Optional.of(usrLibDirectory);
	}

	@Nullable
	private static File deriveFlinkHomeDirectoryFromLibDirectory() {
		final String libDirectory = System.getenv().get(ConfigConstants.ENV_FLINK_LIB_DIR);

		if (libDirectory == null) {
			return null;
		} else {
			return new File(libDirectory).getParentFile();
		}
	}
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Utility class for Yarn tests.
 */
public class YarnTestUtils {

	static boolean isHadoopVersionGreaterThanOrEquals(final int major, final int minor) {
		final String[] splitVersion = VersionInfo.getVersion().split("\\.");
		final int[] versions = Arrays.stream(splitVersion).mapToInt(Integer::parseInt).toArray();
		return versions[0] >= major && versions[1] >= minor;
	}

	private YarnTestUtils() {
		throw new UnsupportedOperationException("This class should never be instantiated.");
	}

	public static YarnClusterDescriptor createClusterDescriptorWithLogging(
			final String flinkConfDir,
			final Configuration flinkConfiguration,
			final YarnConfiguration yarnConfiguration,
			final YarnClient yarnClient,
			final boolean sharedYarnClient) {
		final Configuration effectiveConfiguration = configureLogFile(flinkConfiguration, flinkConfDir);
		return new YarnClusterDescriptor(
			effectiveConfiguration,
			yarnConfiguration,
			yarnClient,
			YarnClientYarnClusterInformationRetriever.create(yarnClient),
			sharedYarnClient);
	}

	public static Configuration configureLogFile(Configuration flinkConfiguration, String flinkConfDir) {
		return YarnLogConfigUtil.setLogConfigFileInConfig(flinkConfiguration, flinkConfDir);
	}

	/**
	 * Creates a series of files with the provided content in the specified directory.
	 * @param directory the directory in which to create the files.
	 * @param srcFiles a map of the relative path to the final file and its desired content.
	 */
	public static void generateFilesInDirectory(
		File directory,
		Map<String, String> srcFiles) throws IOException {

		for (Map.Entry<String, String> src : srcFiles.entrySet()) {
			File file = new File(directory, src.getKey());
			//noinspection ResultOfMethodCallIgnored
			file.getParentFile().mkdirs();
			try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
				out.writeUTF(src.getValue());
			}
		}
	}
}

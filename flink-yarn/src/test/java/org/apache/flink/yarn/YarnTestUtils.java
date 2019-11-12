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
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Arrays;

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
		final Configuration effectiveConfiguration = FlinkYarnSessionCli.setLogConfigFileInConfig(flinkConfiguration, flinkConfDir);
		return new YarnClusterDescriptor(effectiveConfiguration, yarnConfiguration, yarnClient, sharedYarnClient);
	}
}

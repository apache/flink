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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for the deployment of Yarn Flink clusters with customized file replication numbers.
 */
public class YARNFileReplicationITCase extends YARNITCase {

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-per-job");
		startYARNWithConfig(YARN_CONFIGURATION, true);
	}

	@Test
	public void testPerJobModeWithCustomizedFileReplication() throws Exception {
		Configuration configuration = createDefaultConfiguration(YarnConfigOptions.UserJarInclusion.DISABLED);
		configuration.setInteger(YarnConfigOptions.FILE_REPLICATION, 4);

		runTest(() -> deployPerjob(
			configuration,
			getTestingJobGraph()));
	}

	@Test
	public void testPerJobModeWithDefaultFileReplication() throws Exception {
		Configuration configuration = createDefaultConfiguration(YarnConfigOptions.UserJarInclusion.DISABLED);

		runTest(() -> deployPerjob(
			configuration,
			getTestingJobGraph()));
	}

	@Override
	protected void extraVerification(Configuration configuration, ApplicationId applicationId) throws Exception {
		final FileSystem fs = FileSystem.get(getYarnConfiguration());

		String suffix = ".flink/" + applicationId.toString() + "/" + flinkUberjar.getName();

		Path uberJarHDFSPath = new Path(fs.getHomeDirectory(), suffix);
		FileStatus fsStatus = fs.getFileStatus(uberJarHDFSPath);

		final int flinkFileReplication = configuration.getInteger(YarnConfigOptions.FILE_REPLICATION);
		final int replication = YARN_CONFIGURATION.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, DFSConfigKeys.DFS_REPLICATION_DEFAULT);

		// If YarnConfigOptions.FILE_REPLICATION is not set. The replication number should equals to yarn configuration value.
		int expectedReplication = flinkFileReplication > 0
			? flinkFileReplication : replication;
		assertEquals(expectedReplication, fsStatus.getReplication());
	}
}

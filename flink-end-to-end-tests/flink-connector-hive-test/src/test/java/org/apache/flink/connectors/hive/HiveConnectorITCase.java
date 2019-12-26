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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.connectors.hive.tests.HiveReadWriteDataTest;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.JobSubmission;
import org.apache.flink.tests.util.hive.YarnClusterAndHiveDockerResource;
import org.apache.flink.tests.util.hive.YarnClusterAndHiveResource;
import org.apache.flink.tests.util.hive.YarnClusterFlinkResource;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Path;

/**
 * A test case used to test hive connector, hive meta and other function in an end to end way.
 */
@Category(value = {TravisGroup1.class, FailsOnJava11.class})
public class HiveConnectorITCase extends TestLogger {
	private static String hiveVersion = "2.3.6";
	private static String hadoopVersion = "2.8.5";
	private static Path testJarPath;

	@ClassRule
	public static YarnClusterAndHiveResource clusterAndHiveResource =
			new YarnClusterAndHiveDockerResource(hiveVersion, hadoopVersion);

	@ClassRule
	public static YarnClusterFlinkResource flinkResource =
			new YarnClusterFlinkResource(clusterAndHiveResource);

	@BeforeClass
	public static void beforeClass() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 120000);
		configuration.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 100);
		flinkResource.addConfiguration(configuration);
		testJarPath = TestUtils.getResourceJar("/testHive.jar");
	}

	@Test
	public void testSimpleReadWriteHiveTable() throws Exception {
		clusterAndHiveResource.execHiveSql("create database test_db1");
		clusterAndHiveResource.execHiveSql("create database test_db2");
		clusterAndHiveResource.execHiveSql("CREATE TABLE non_partition_table ( " +
											"a INT, b INT, c STRING, d BIGINT, e DOUBLE) " +
											"row format delimited fields terminated by ','");
		String localPath = getClass().getResource("/test-data/non_partition_table.txt").getFile();
		clusterAndHiveResource.copyLocalFileToHiveGateWay(localPath,
				"/tmp/test-data/non_partition_table.txt");
		clusterAndHiveResource.execHiveSql("load data local inpath '/tmp/test-data/non_partition_table.txt' " +
											"into table non_partition_table");
		clusterAndHiveResource.execHiveSql("CREATE TABLE dest_non_partition_table" +
											" ( a INT, b INT, c STRING, d BIGINT, e DOUBLE)");
		try (final ClusterController clusterController = flinkResource.startCluster(1)) {
			JobSubmission.JobSubmissionBuilder jobSubmissionBuilder = new JobSubmission.JobSubmissionBuilder(testJarPath);
			jobSubmissionBuilder.setParallelism(1)
					.setDetached(false)
					.addOption("-ys", "1")
					.addOption("-ytm", "1000")
					.addOption("-yjm", "1000")
					.addOption("-c", HiveReadWriteDataTest.class.getCanonicalName())
					.addArgument("--hiveVersion", hiveVersion)
					.addArgument("--sourceTable", "non_partition_table")
					.addArgument("--targetTable", "dest_non_partition_table");
			YarnClusterFlinkResource.YarnClusterJobController  jobController =
					(YarnClusterFlinkResource.YarnClusterJobController) clusterController.submitJob(jobSubmissionBuilder.build());
			log.info(jobController.fetchExecuteLog());
		}
		String expectedResults = clusterAndHiveResource.execHiveSql("select * from non_partition_table");
		String actualResults = clusterAndHiveResource.execHiveSql("select * from dest_non_partition_table");
		Assert.assertEquals(expectedResults, actualResults);
	}
}

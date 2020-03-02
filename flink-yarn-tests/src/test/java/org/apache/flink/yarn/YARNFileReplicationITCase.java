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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.util.YarnTestUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.yarn.configuration.YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test cases for the deployment of Yarn Flink clusters with customized file replication numbers.
 */
public class YARNFileReplicationITCase extends YarnTestBase {

	private static final Duration yarnAppTerminateTimeout = Duration.ofSeconds(10);
	private static final int sleepIntervalInMS = 100;

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-per-job");
		startYARNWithConfig(YARN_CONFIGURATION, true);
	}

	@Test
	public void testPerJobModeWithCustomizedFileReplication() throws Exception {
		final Configuration configuration = getDefaultConfiguration();
		configuration.setInteger(YarnConfigOptions.FILE_REPLICATION, 4);

		runTest(() -> deployPerJob(configuration, getTestingJobGraph()));
	}

	@Test
	public void testPerJobModeWithDefaultFileReplication() throws Exception {
		runTest(() -> deployPerJob(getDefaultConfiguration(), getTestingJobGraph()));
	}

	private void deployPerJob(Configuration configuration, JobGraph jobGraph) throws Exception {
		try (final YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor(configuration)) {

			yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
			yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));
			yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkShadedHadoopDir.listFiles()));

			final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
				.setMasterMemoryMB(768)
				.setTaskManagerMemoryMB(1024)
				.setSlotsPerTaskManager(1)
				.createClusterSpecification();

			File testingJar = YarnTestBase.findFile("..", new YarnTestUtils.TestJarFinder("flink-yarn-tests"));

			jobGraph.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));
			try (ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor
				.deployJobCluster(
					clusterSpecification,
					jobGraph,
					false)
				.getClusterClient()) {

				ApplicationId applicationId = clusterClient.getClusterId();

				final CompletableFuture<JobResult> jobResultCompletableFuture = clusterClient.requestJobResult(jobGraph.getJobID());

				final JobResult jobResult = jobResultCompletableFuture.get();

				assertThat(jobResult, is(notNullValue()));
				assertThat(jobResult.getSerializedThrowable().isPresent(), is(false));

				extraVerification(configuration, applicationId);

				waitApplicationFinishedElseKillIt(
					applicationId, yarnAppTerminateTimeout, yarnClusterDescriptor, sleepIntervalInMS);
			}
		}
	}

	private JobGraph getTestingJobGraph() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		env.addSource(new NoDataSource())
			.shuffle()
			.addSink(new DiscardingSink<>());

		return env.getStreamGraph().getJobGraph();
	}

	private Configuration getDefaultConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
		configuration.setString(AkkaOptions.ASK_TIMEOUT, "30 s");
		configuration.setString(CLASSPATH_INCLUDE_USER_JAR, YarnConfigOptions.UserJarInclusion.DISABLED.toString());

		return configuration;
	}

	private void extraVerification(Configuration configuration, ApplicationId applicationId) throws Exception {
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

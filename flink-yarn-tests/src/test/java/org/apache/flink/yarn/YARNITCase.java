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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.testjob.YarnTestCacheJob;
import org.apache.flink.yarn.util.YarnTestUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.yarn.configuration.YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Test cases for the deployment of Yarn Flink clusters.
 */
public class YARNITCase extends YarnTestBase {

	private final Duration yarnAppTerminateTimeout = Duration.ofSeconds(10);

	private final int sleepIntervalInMS = 100;

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-per-job");
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@Test
	public void testPerJobModeWithEnableSystemClassPathIncludeUserJar() throws Exception {
		runTest(() -> deployPerjob(YarnConfigOptions.UserJarInclusion.FIRST, getTestingJobGraph()));
	}

	@Test
	public void testPerJobModeWithDisableSystemClassPathIncludeUserJar() throws Exception {
		runTest(() -> deployPerjob(YarnConfigOptions.UserJarInclusion.DISABLED, getTestingJobGraph()));
	}

	@Test
	public void testPerJobModeWithDistributedCache() throws Exception {
		runTest(() -> deployPerjob(
			YarnConfigOptions.UserJarInclusion.DISABLED,
			YarnTestCacheJob.getDistributedCacheJobGraph(tmp.newFolder())));
	}

	private void deployPerjob(YarnConfigOptions.UserJarInclusion userJarInclusion, JobGraph jobGraph) throws Exception {

		Configuration configuration = new Configuration();
		configuration.setString(AkkaOptions.ASK_TIMEOUT, "30 s");
		configuration.setString(CLASSPATH_INCLUDE_USER_JAR, userJarInclusion.toString());

		try (final YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor(configuration)) {

			yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
			yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));
			yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkShadedHadoopDir.listFiles()));

			final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
				.setMasterMemoryMB(768)
				.setTaskManagerMemoryMB(1024)
				.setSlotsPerTaskManager(1)
				.setNumberTaskManagers(1)
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
}

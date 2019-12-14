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
import org.apache.flink.yarn.testjob.YarnTestCacheJob;
import org.apache.flink.yarn.util.YarnTestUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Test cases for the deployment of Yarn Flink with Distributed Cache.
 */
public class YarnDistributedCacheITCase extends YarnTestBase {

	private final Duration yarnAppTerminateTimeout = Duration.ofSeconds(10);

	private final int sleepIntervalInMS = 100;

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-with-distributed-cache");
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@Test
	public void testPerJobModeWithDistributedCache() throws Exception {
		runTest(() -> {
			Configuration configuration = new Configuration();
			configuration.setString(AkkaOptions.ASK_TIMEOUT, "30 s");
			final YarnClient yarnClient = getYarnClient();

			try (final YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
				configuration,
				getYarnConfiguration(),
				yarnClient,
				true)) {

				yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
				yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));
				yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkShadedHadoopDir.listFiles()));

				final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
					.setMasterMemoryMB(768)
					.setTaskManagerMemoryMB(1024)
					.setSlotsPerTaskManager(1)
					.setNumberTaskManagers(1)
					.createClusterSpecification();

				final JobGraph jobGraph = YarnTestCacheJob.getDistributedCacheJobGraph();

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

					waitApplicationFinishedElseKillIt(applicationId, yarnAppTerminateTimeout, yarnClusterDescriptor, sleepIntervalInMS);
				}
			}
		});
	}
}

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
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Test cases for the deployment of Yarn Flink clusters.
 */
public class YARNITCase extends YarnTestBase {

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-ha");
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@Test
	public void testPerJobMode() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(AkkaOptions.ASK_TIMEOUT, "30 s");
		final YarnClient yarnClient = getYarnClient();

		try (final YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
			configuration,
			getYarnConfiguration(),
			System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR),
			yarnClient,
			true)) {

			yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
			yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));

			final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
				.setMasterMemoryMB(768)
				.setTaskManagerMemoryMB(1024)
				.setSlotsPerTaskManager(1)
				.setNumberTaskManagers(1)
				.createClusterSpecification();

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(2);

			env.addSource(new NoDataSource())
				.shuffle()
				.addSink(new DiscardingSink<>());

			final JobGraph jobGraph = env.getStreamGraph().getJobGraph();

			File testingJar = YarnTestBase.findFile("..", new TestingYarnClusterDescriptor.TestJarFinder("flink-yarn-tests"));

			jobGraph.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));

			ApplicationId applicationId = null;
			ClusterClient<ApplicationId> clusterClient = null;

			try {
				clusterClient = yarnClusterDescriptor.deployJobCluster(
					clusterSpecification,
					jobGraph,
					false);
				applicationId = clusterClient.getClusterId();

				assertThat(clusterClient, is(instanceOf(RestClusterClient.class)));
				final RestClusterClient<ApplicationId> restClusterClient = (RestClusterClient<ApplicationId>) clusterClient;

				final CompletableFuture<JobResult> jobResultCompletableFuture = restClusterClient.requestJobResult(jobGraph.getJobID());

				final JobResult jobResult = jobResultCompletableFuture.get();

				assertThat(jobResult, is(notNullValue()));
				assertThat(jobResult.getSerializedThrowable().isPresent(), is(false));
			} finally {
				if (clusterClient != null) {
					clusterClient.shutdown();
				}

				if (applicationId != null) {
					yarnClusterDescriptor.killCluster(applicationId);
				}
			}
		}
	}

	private static class NoDataSource implements ParallelSourceFunction<Integer> {

		private static final long serialVersionUID = 1642561062000662861L;

		@Override
		public void run(SourceContext<Integer> ctx) {}

		@Override
		public void cancel() {}
	}
}

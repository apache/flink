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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.util.YarnTestUtils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.yarn.configuration.YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test cases for the deployment of Yarn Flink clusters.
 */
public class YARNITCase extends YarnTestBase {

	private final Duration yarnAppTerminateTimeout = Duration.ofSeconds(10);

	private final int sleepIntervalInMS = 100;

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("/tmp"));

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-per-job");
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@Test
	public void testPerJobModeWithEnableSystemClassPathIncludeUserJar() throws Exception {
		runTest(() -> deployPerjob(YarnConfigOptions.UserJarInclusion.FIRST, null));
	}

	@Test
	public void testPerJobModeWithDisableSystemClassPathIncludeUserJar() throws Exception {
		runTest(() -> deployPerjob(YarnConfigOptions.UserJarInclusion.DISABLED, null));
	}

	@Test
	public void testPreUploadedFlinkPath() throws Exception {
		// Prepare a pre-uploaded flink binary, it should be publicly accessible. So put it under /tmp folder.
		File tmpFlinkReleaseBinary = temporaryFolder.newFolder();

		FileUtils.copyDirectory(flinkLibFolder.getParentFile(), tmpFlinkReleaseBinary);

		runTest(() -> deployPerjob(YarnConfigOptions.UserJarInclusion.FIRST, tmpFlinkReleaseBinary));
	}

	private void deployPerjob(
		YarnConfigOptions.UserJarInclusion userJarInclusion, @Nullable File preUploadedFlinkBinary) throws Exception {

		Configuration configuration = new Configuration();
		configuration.setString(AkkaOptions.ASK_TIMEOUT, "30 s");
		configuration.setString(CLASSPATH_INCLUDE_USER_JAR, userJarInclusion.toString());

		if (preUploadedFlinkBinary != null) {
			configuration.setString(YarnConfigOptions.PRE_UPLOADED_FLINK_PATH, preUploadedFlinkBinary.toURI().toString());
		}

		final YarnClient yarnClient = getYarnClient();

		try (final YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
			configuration,
			getYarnConfiguration(),
			yarnClient,
			true)) {

			yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
			yarnClusterDescriptor.addShipFiles(Collections.singletonList(flinkLibFolder));
			yarnClusterDescriptor.addShipFiles(Collections.singletonList(flinkShadedHadoopDir));

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

			File testingJar = YarnTestBase.findFile("..", new YarnTestUtils.TestJarFinder("flink-yarn-tests"));

			jobGraph.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));
			try (ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor.deployJobCluster(
				clusterSpecification,
				jobGraph,
				false)) {

				ApplicationId applicationId = clusterClient.getClusterId();

				final CompletableFuture<JobResult> jobResultCompletableFuture = clusterClient.requestJobResult(jobGraph.getJobID());

				final JobResult jobResult = jobResultCompletableFuture.get();

				assertThat(jobResult, is(notNullValue()));
				assertThat(jobResult.getSerializedThrowable().isPresent(), is(false));

				checkStagingDirectory(preUploadedFlinkBinary == null, applicationId);

				waitApplicationFinishedElseKillIt(applicationId, yarnAppTerminateTimeout, yarnClusterDescriptor);
			}
		}
	}

	private void checkStagingDirectory(boolean shouldExist, ApplicationId appId) throws IOException {
		final FileSystem fs = FileSystem.get(YARN_CONFIGURATION);
		final Path stagingDirectory = new Path(fs.getHomeDirectory(), ".flink/" + appId.toString());
		if (shouldExist) {
			assertTrue("The lib directory should be uploaded to staging directory.",
				fs.exists(new Path(stagingDirectory, flinkLibFolder.getName())));
		} else {
			assertFalse("The pre-uploaded flink is set, so the lib directory should not be uploaded to " +
				"staging directory.", fs.exists(new Path(stagingDirectory, flinkLibFolder.getName())));
		}
	}

	private void waitApplicationFinishedElseKillIt(
			ApplicationId applicationId,
			Duration timeout,
			YarnClusterDescriptor yarnClusterDescriptor) throws Exception {
		Deadline deadline = Deadline.now().plus(timeout);
		YarnApplicationState state = getYarnClient().getApplicationReport(applicationId).getYarnApplicationState();

		while (state != YarnApplicationState.FINISHED) {
			if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
				fail("Application became FAILED or KILLED while expecting FINISHED");
			}

			if (deadline.isOverdue()) {
				yarnClusterDescriptor.killCluster(applicationId);
				fail("Application didn't finish before timeout");
			}

			sleep(sleepIntervalInMS);
			state = getYarnClient().getApplicationReport(applicationId).getYarnApplicationState();
		}
	}

}

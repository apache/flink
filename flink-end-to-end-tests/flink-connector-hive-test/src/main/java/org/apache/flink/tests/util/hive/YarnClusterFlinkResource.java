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

package org.apache.flink.tests.util.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.tests.util.AutoClosableProcess;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.JobController;
import org.apache.flink.tests.util.flink.JobSubmission;
import org.apache.flink.tests.util.flink.SQLJobSubmission;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * YarnClusterFlinkResource use the {@link YarnClusterAndHiveResource} as the yarn cluster to submit flink job in
 * per-job mode.
 */
public class YarnClusterFlinkResource implements FlinkResource {

	private static final Logger LOG = LoggerFactory.getLogger(YarnClusterFlinkResource.class);
	private final YarnClusterAndHiveResource yarnCluster;
	private final TemporaryFolder temporaryFolder = new TemporaryFolder();
	private final Path originalFlinkDir;
	private boolean deployFlinkToRemote = false;
	public final String remoteFlinkDir = "/home/hadoop-user/flink";
	Path localFlinkDir;
	private Configuration defaultConfig;
	private Path conf;

	public YarnClusterFlinkResource(YarnClusterAndHiveResource yarnCluster) {
		this.yarnCluster = yarnCluster;
		final String distDirProperty = System.getProperty("distDir");
		if (distDirProperty == null) {
			Assert.fail("The distDir property was not set. You can set it when running maven via -DdistDir=<path> .");
		}
		originalFlinkDir = Paths.get(distDirProperty);
	}

	@Override
	public void addConfiguration(Configuration config) throws IOException {
		final Configuration mergedConfig = new Configuration();
		mergedConfig.addAll(defaultConfig);
		mergedConfig.addAll(config);

		final List<String> configurationLines = mergedConfig.toMap().entrySet().stream()
				.map(entry -> entry.getKey() + ": " + entry.getValue())
				.collect(Collectors.toList());

		Files.write(conf.resolve("flink-conf.yaml"), configurationLines);
	}

	@Override
	public ClusterController startCluster(int numTaskManagers) throws IOException {
		if (!deployFlinkToRemote) {
			yarnCluster.copyLocalFileToYarnMaster(localFlinkDir.toAbsolutePath().toString(), remoteFlinkDir);
			deployFlinkToRemote = true;
		}
		return new YarnClusterController(this, yarnCluster, numTaskManagers);
	}

	@Override
	public void before() throws Exception {
		temporaryFolder.create();
		localFlinkDir = temporaryFolder.newFolder().toPath();

		LOG.info("Copying distribution to {}.", localFlinkDir);
		TestUtils.copyDirectory(originalFlinkDir, localFlinkDir);
		conf = localFlinkDir.resolve("conf");
		defaultConfig = new UnmodifiableConfiguration(GlobalConfiguration.loadConfiguration(conf.toAbsolutePath().toString()));
	}

	@Override
	public void afterTestSuccess() {
		temporaryFolder.delete();
	}

	@Override
	public void afterTestFailure() {
		temporaryFolder.delete();
	}

	/**
	 * YarnClusterController used to submit job in yarn per-job mode.
	 */
	private static class YarnClusterController implements ClusterController {
		private final YarnClusterFlinkResource flinkResource;
		private final YarnClusterAndHiveResource yarnClusterAndHiveResource;
		private final int numTaskManagers;

		public YarnClusterController(
				YarnClusterFlinkResource flinkResource,
				YarnClusterAndHiveResource yarnClusterAndHiveResource,
				int numTaskManagers) {
			this.flinkResource = flinkResource;
			this.yarnClusterAndHiveResource = yarnClusterAndHiveResource;
			this.numTaskManagers = numTaskManagers;
		}

		/**
		 * Submits the given job to the cluster.
		 *
		 * @param job job to submit
		 * @return JobController for the submitted job
		 * @throws IOException
		 */
		@Override
		public JobController submitJob(JobSubmission job) throws IOException {
			Path jobJarPath = job.getJar();
			String targetPath = String.format("/tmp/%s-%s", UUID.randomUUID(), jobJarPath.toFile().getName());
			yarnClusterAndHiveResource.copyLocalFileToYarnMaster(
					jobJarPath.toAbsolutePath().toString(), targetPath);
			List<String> commands = new ArrayList<>();
			commands.add("docker");
			commands.add("exec");
			commands.add(YarnClusterAndHiveDockerResource.ContainerRole.YARN_MASTER.getContainerName());
			commands.add("bash");
			commands.add("-c");
			StringBuilder flinkRunCommand = new StringBuilder();
			flinkRunCommand.append("export HADOOP_CLASSPATH=`hadoop classpath` && ");
			flinkRunCommand.append(String.format("%s/bin/flink run -m yarn-cluster ", flinkResource.remoteFlinkDir));
			flinkRunCommand.append(StringUtils.join(job.getOptions(), " "));
			flinkRunCommand.append(String.format(" %s ", targetPath));
			flinkRunCommand.append(StringUtils.join(job.getArguments(), " "));
			commands.add(flinkRunCommand.toString());
			AutoClosableProcess.AutoClosableProcessBuilder autoClosableProcessBuilder =
					AutoClosableProcess.create(commands.toArray(new String[commands.size()]));
			List<String> lines = new ArrayList<>();
			autoClosableProcessBuilder.setStdoutProcessor(s -> lines.add(s));
			autoClosableProcessBuilder.runBlocking(Duration.ofMinutes(10));
			return new YarnClusterJobController(lines);
		}

		@Override
		public void submitSQLJob(SQLJobSubmission job) throws IOException {

		}

		/**
		 * Trigger the closing of the resource and return the corresponding
		 * close future.
		 *
		 * @return Future which is completed once the resource has been closed
		 */
		@Override
		public CompletableFuture<Void> closeAsync() {
			return CompletableFuture.completedFuture(null);
		}
	}

	/**
	 * YarnClusterJobController can be used to fetch the execute log.
	 */
	public static class YarnClusterJobController implements JobController {
		private List<String> lines;

		public YarnClusterJobController(List<String> lines) {
			this.lines = lines;
		}

		public String fetchExecuteLog() {
			return StringUtils.join(lines, "\n");
		}
	}
}

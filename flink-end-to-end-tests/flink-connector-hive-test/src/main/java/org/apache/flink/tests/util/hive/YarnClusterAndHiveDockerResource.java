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

import org.apache.flink.tests.util.AutoClosableProcess;
import org.apache.flink.tests.util.CommandLineWrapper;
import org.apache.flink.tests.util.activation.OperatingSystemRestriction;
import org.apache.flink.util.OperatingSystem;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A docker based YarnCluster and hive service wrapper resource.
 */
public class YarnClusterAndHiveDockerResource implements YarnClusterAndHiveResource {

	/**
	 * Enum to represent the different role in this yarn cluster and hive service.
	 */
	public enum ContainerRole {
		YARN_MASTER("master"),
		YARN_SLAVE1("slave1"),
		YARN_SLAVE2("slave2"),
		MYSQL("mysql"),
		HIVE("hive");

		private String containerName;

		ContainerRole(String containerName) {
			this.containerName = containerName;
		}

		public String getContainerName() {
			return containerName;
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(YarnClusterAndHiveResource.class);
	private final String hiveVersion;
	private final String hadoopVersion;
	private final String dockerFilePath;

	public YarnClusterAndHiveDockerResource(String hiveVersion, String hadoopVersion) {
		OperatingSystemRestriction.forbid(
				String.format("The %s relies on UNIX utils and shell scripts.", getClass().getSimpleName()),
				OperatingSystem.WINDOWS);
		this.hiveVersion = hiveVersion;
		this.hadoopVersion = hadoopVersion;
		dockerFilePath = getClass().getClassLoader().getResource("docker-hive-hadoop-cluster").getFile();
	}

	private void buildDockerImage() throws IOException {
		LOG.info("begin to build YarnHiveDockerImage!");
		AutoClosableProcess.runBlocking(
				Duration.ofMinutes(10),
				new CommandLineWrapper.DockerBuildBuilder()
						.buildArg(String.format("HADOOP_VERSION=%s", hadoopVersion))
						.buildArg(String.format("HIVE_VERSION=%s", hiveVersion))
						.tag("flink/flink-hadoop-hive-cluster:latest")
						.buildPath(dockerFilePath)
						.build());
	}

	@Override
	public void startYarnClusterAndHiveServer() throws IOException{
		LOG.info("begin to start yarn cluster and hive server");
		String[] startCommands = String.format("docker-compose -f %s/docker-compose.yml up -d", dockerFilePath)
				.split("\\s+");
		AutoClosableProcess.runBlocking(Duration.ofMinutes(10), startCommands);
		String[] waitCommands = String.format("bash %s/wait_yarn_cluster_hive_start.sh", dockerFilePath)
				.split("\\s+");
		AutoClosableProcess.runBlocking(Duration.ofMinutes(5), waitCommands);
		LOG.info("Start yarn cluster and hive server success");
	}

	@Override
	public void stopYarnClusterAndHiveServer() throws IOException {
		LOG.info("begin to stop yarn cluster and hive server");
		String[] commands = String.format("docker-compose -f %s/docker-compose.yml down", dockerFilePath)
				.split("\\s+");
		AutoClosableProcess.runBlocking(Duration.ofMinutes(1), commands);
		LOG.info("Stop yarn cluster and hive server success");
	}

	@Override
	public String execHiveSql(String sql) throws IOException {
		LOG.debug(String.format("execute sql:%s on hive container", sql));
		String[] commands = new DockerExecBuilder(ContainerRole.HIVE.getContainerName())
				.command("hive").arg("-e").arg(sql).build();
		AutoClosableProcess.AutoClosableProcessBuilder autoClosableProcessBuilder =
				AutoClosableProcess.create(commands);
		List<String> lines = new ArrayList<>();
		autoClosableProcessBuilder.setStdoutProcessor(s -> lines.add(s));
		autoClosableProcessBuilder.runBlocking(Duration.ofMinutes(3));
		return StringUtils.join(lines, "\n");
	}

	@Override
	public void copyLocalFileToHiveGateWay(String localPath, String remotePath) throws IOException {
		LOG.debug(String.format("copy localPath %s to hive container path %s", localPath, remotePath));
		String[] commands = String.format("docker cp %s %s:%s",
				localPath, ContainerRole.HIVE.getContainerName(), remotePath).split("\\s+");
		AutoClosableProcess.runBlocking(commands);
	}

	@Override
	public void copyLocalFileToYarnMaster(String localPath, String remotePath) throws IOException {
		LOG.debug(String.format("copy localPath %s to yarn master path %s", localPath, remotePath));
		String[] commands = String.format("docker cp %s %s:%s",
				localPath, ContainerRole.YARN_MASTER.getContainerName(), remotePath).split("\\s+");
		AutoClosableProcess.runBlocking(commands);
	}

	@Override
	public void before() throws Exception {
		buildDockerImage();
		startYarnClusterAndHiveServer();
	}

	@Override
	public void afterTestSuccess() {
		try {
			stopYarnClusterAndHiveServer();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Wrapper around docker exec used for running command in docker.
	 */
	public static final class DockerExecBuilder{
		private final String containerName;
		private final List<String> commandsList = new ArrayList<>();
		private String command;
		private List<String> args = new ArrayList<>();

		public DockerExecBuilder(String containerName) {
			this.containerName = containerName;
		}

		public DockerExecBuilder command(String command) {
			this.command = command;
			return this;
		}

		public DockerExecBuilder arg(String arg) {
			args.add(arg);
			return this;
		}

		public String[] build() {
			Objects.requireNonNull(containerName);
			Objects.requireNonNull(command);
			commandsList.add("docker");
			commandsList.add("exec");
			commandsList.add(containerName);
			commandsList.add(command);
			commandsList.addAll(args);
			return commandsList.toArray(new String[commandsList.size()]);
		}
	}
}

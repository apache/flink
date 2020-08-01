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

import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.GenericCLI;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.executors.YarnJobClusterExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link FlinkYarnSessionCli}.
 */
public class FlinkYarnSessionCliTest extends TestLogger {

	private static final ApplicationId TEST_YARN_APPLICATION_ID = ApplicationId.newInstance(System.currentTimeMillis(), 42);

	private static final ApplicationId TEST_YARN_APPLICATION_ID_2 = ApplicationId.newInstance(System.currentTimeMillis(), 43);

	private static final String TEST_YARN_JOB_MANAGER_ADDRESS = "22.33.44.55";
	private static final int TEST_YARN_JOB_MANAGER_PORT = 6655;

	private static final String validPropertiesFile = "applicationID=" + TEST_YARN_APPLICATION_ID;

	private static final String invalidPropertiesFile = "jasfobManager=" + TEST_YARN_JOB_MANAGER_ADDRESS + ":asf" + TEST_YARN_JOB_MANAGER_PORT;

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testDynamicProperties() throws Exception {

		FlinkYarnSessionCli cli = new FlinkYarnSessionCli(
			new Configuration(),
			tmp.getRoot().getAbsolutePath(),
			"",
			"",
			false);
		Options options = new Options();
		cli.addGeneralOptions(options);
		cli.addRunOptions(options);

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, new String[]{"run", "-j", "fake.jar",
				"-D", AkkaOptions.ASK_TIMEOUT.key() + "=5 min",
				"-D", CoreOptions.FLINK_JVM_OPTIONS.key() + "=-DappName=foobar",
				"-D", SecurityOptions.SSL_INTERNAL_KEY_PASSWORD.key() + "=changeit"});

		Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(cmd);
		assertEquals("5 min", executorConfig.get(AkkaOptions.ASK_TIMEOUT));
		assertEquals("-DappName=foobar", executorConfig.get(CoreOptions.FLINK_JVM_OPTIONS));
		assertEquals("changeit", executorConfig.get(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD));
	}

	@Test
	public void testCorrectSettingOfMaxSlots() throws Exception {
		String[] params =
			new String[] {"-ys", "3"};

		FlinkYarnSessionCli yarnCLI = createFlinkYarnSessionCliWithJmAndTmTotalMemory(2048);

		final CommandLine commandLine = yarnCLI.parseCommandLineOptions(params, true);

		final Configuration executorConfig = yarnCLI.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		// each task manager has 3 slots but the parallelism is 7. Thus the slots should be increased.
		assertEquals(3, clusterSpecification.getSlotsPerTaskManager());
	}

	@Test
	public void testCorrectSettingOfDetachedMode() throws Exception {
		final String[] params = new String[] {"-yd"};
		FlinkYarnSessionCli yarnCLI = createFlinkYarnSessionCli();

		final CommandLine commandLine = yarnCLI.parseCommandLineOptions(params, true);
		final Configuration executorConfig = yarnCLI.applyCommandLineOptionsToConfiguration(commandLine);

		assertThat(executorConfig.get(DeploymentOptions.ATTACHED), is(false));
	}

	@Test
	public void testZookeeperNamespaceProperty() throws Exception {
		String zkNamespaceCliInput = "flink_test_namespace";

		String[] params = new String[] {"-yz", zkNamespaceCliInput};

		FlinkYarnSessionCli yarnCLI = createFlinkYarnSessionCli();

		CommandLine commandLine = yarnCLI.parseCommandLineOptions(params, true);

		Configuration executorConfig = yarnCLI.applyCommandLineOptionsToConfiguration(commandLine);
		ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		YarnClusterDescriptor descriptor = (YarnClusterDescriptor) clientFactory.createClusterDescriptor(executorConfig);

		assertEquals(zkNamespaceCliInput, descriptor.getZookeeperNamespace());
	}

	@Test
	public void testNodeLabelProperty() throws Exception {
		String nodeLabelCliInput = "flink_test_nodelabel";

		String[] params = new String[] {"-ynl", nodeLabelCliInput };

		FlinkYarnSessionCli yarnCLI = createFlinkYarnSessionCli();

		CommandLine commandLine = yarnCLI.parseCommandLineOptions(params, true);

		Configuration executorConfig = yarnCLI.applyCommandLineOptionsToConfiguration(commandLine);
		ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		YarnClusterDescriptor descriptor = (YarnClusterDescriptor) clientFactory.createClusterDescriptor(executorConfig);

		assertEquals(nodeLabelCliInput, descriptor.getNodeLabel());
	}

	@Test
	public void testExecutorCLIisPrioritised() throws Exception {
		final File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		final Configuration configuration = new Configuration();
		configuration.setString(YarnConfigOptions.PROPERTIES_FILE_LOCATION, directoryPath.getAbsolutePath());

		validateYarnCLIisActive(configuration);

		final String[] argsUnderTest = new String[] {"-e", YarnJobClusterExecutor.NAME};

		validateExecutorCLIisPrioritised(configuration, argsUnderTest);
	}

	private void validateExecutorCLIisPrioritised(Configuration configuration, String[] argsUnderTest) throws IOException, CliArgsException {
		final List<CustomCommandLine> customCommandLines = CliFrontend.loadCustomCommandLines(
				configuration,
				tmp.newFile().getAbsolutePath());

		final CliFrontend cli = new CliFrontend(configuration, customCommandLines);
		final CommandLine commandLine = cli.getCommandLine(
				CliFrontendParser.getRunCommandOptions(),
				argsUnderTest,
				true);

		final CustomCommandLine customCommandLine = cli.validateAndGetActiveCommandLine(commandLine);
		assertTrue(customCommandLine instanceof GenericCLI);
	}

	private void validateYarnCLIisActive(Configuration configuration) throws FlinkException, CliArgsException {
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli(configuration);
		final CommandLine testCLIArgs = flinkYarnSessionCli.parseCommandLineOptions(new String[] {}, true);
		assertTrue(flinkYarnSessionCli.isActive(testCLIArgs));
	}

	/**
	 * Test that the CliFrontend is able to pick up the .yarn-properties file from a specified location.
	 */
	@Test
	public void testResumeFromYarnPropertiesFile() throws Exception {

		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		final Configuration configuration = new Configuration();
		configuration.setString(YarnConfigOptions.PROPERTIES_FILE_LOCATION, directoryPath.getAbsolutePath());

		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli(configuration);

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {}, true);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final ApplicationId clusterId = clientFactory.getClusterId(executorConfig);

		assertEquals(TEST_YARN_APPLICATION_ID, clusterId);
	}

	/**
	 * Tests that we fail when reading an invalid yarn properties file when retrieving
	 * the cluster id.
	 */
	@Test(expected = FlinkException.class)
	public void testInvalidYarnPropertiesFile() throws Exception {

		File directoryPath = writeYarnPropertiesFile(invalidPropertiesFile);

		final Configuration configuration = new Configuration();
		configuration.setString(YarnConfigOptions.PROPERTIES_FILE_LOCATION, directoryPath.getAbsolutePath());

		createFlinkYarnSessionCli(configuration);
	}

	@Test
	public void testResumeFromYarnID() throws Exception {
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli();

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString()}, true);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final ApplicationId clusterId = clientFactory.getClusterId(executorConfig);

		assertEquals(TEST_YARN_APPLICATION_ID, clusterId);
	}

	@Test
	public void testResumeFromYarnIDZookeeperNamespace() throws Exception {
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli();

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString()}, true);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final YarnClusterDescriptor clusterDescriptor = (YarnClusterDescriptor) clientFactory.createClusterDescriptor(executorConfig);

		final Configuration clusterDescriptorConfiguration = clusterDescriptor.getFlinkConfiguration();

		String zkNs = clusterDescriptorConfiguration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);
		assertTrue(zkNs.matches("application_\\d+_0042"));
	}

	@Test
	public void testResumeFromYarnIDZookeeperNamespaceOverride() throws Exception {
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli();

		final String overrideZkNamespace = "my_cluster";

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString(), "-yz", overrideZkNamespace}, true);
		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);

		final YarnClusterDescriptor clusterDescriptor = (YarnClusterDescriptor) clientFactory.createClusterDescriptor(executorConfig);

		final Configuration clusterDescriptorConfiguration = clusterDescriptor.getFlinkConfiguration();
		final String clusterId = clusterDescriptorConfiguration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);
		assertEquals(overrideZkNamespace, clusterId);
	}

	@Test
	public void testYarnIDOverridesPropertiesFile() throws Exception {
		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		final Configuration configuration = new Configuration();
		configuration.setString(YarnConfigOptions.PROPERTIES_FILE_LOCATION, directoryPath.getAbsolutePath());

		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli(configuration);
		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {"-yid", TEST_YARN_APPLICATION_ID_2.toString() }, true);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final ApplicationId clusterId = clientFactory.getClusterId(executorConfig);

		assertEquals(TEST_YARN_APPLICATION_ID_2, clusterId);
	}

	/**
	 * Tests that the command line arguments override the configuration settings
	 * when the {@link ClusterSpecification} is created.
	 */
	@Test
	public void testCommandLineClusterSpecification() throws Exception {
		final Configuration configuration = new Configuration();
		final int jobManagerMemory = 1337;
		final int taskManagerMemory = 7331;
		final int slotsPerTaskManager = 30;

		configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(jobManagerMemory));
		configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(taskManagerMemory));
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);

		final String[] args = {"-yjm", String.valueOf(jobManagerMemory) + "m", "-ytm", String.valueOf(taskManagerMemory) + "m", "-ys", String.valueOf(slotsPerTaskManager)};
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli(configuration);

		CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(args, false);

		Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(jobManagerMemory));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(taskManagerMemory));
		assertThat(clusterSpecification.getSlotsPerTaskManager(), is(slotsPerTaskManager));
	}

	/**
	 * Tests that the configuration settings are used to create the
	 * {@link ClusterSpecification}.
	 */
	@Test
	public void testConfigurationClusterSpecification() throws Exception {
		final Configuration configuration = new Configuration();
		final int jobManagerMemory = 1337;
		configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(jobManagerMemory));
		final int taskManagerMemory = 7331;
		configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(taskManagerMemory));
		final int slotsPerTaskManager = 42;
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);

		final String[] args = {};
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli(configuration);

		CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(args, false);

		Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(jobManagerMemory));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(taskManagerMemory));
		assertThat(clusterSpecification.getSlotsPerTaskManager(), is(slotsPerTaskManager));
	}

	/**
	 * Tests the specifying total process memory without unit for job manager and task manager.
	 */
	@Test
	public void testMemoryPropertyWithoutUnit() throws Exception {
		final String[] args = new String[] { "-yjm", "1024", "-ytm", "2048" };
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli();

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(args, false);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(1024));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(2048));
	}

	/**
	 * Tests the specifying total process memory with unit (MB) for job manager and task manager.
	 */
	@Test
	public void testMemoryPropertyWithUnitMB() throws Exception {
		final String[] args = new String[] { "-yjm", "1024m", "-ytm", "2048m" };
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli();
		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(args, false);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(1024));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(2048));
	}

	/**
	 * Tests the specifying total process memory with arbitrary unit for job manager and task manager.
	 */
	@Test
	public void testMemoryPropertyWithArbitraryUnit() throws Exception {
		final String[] args = new String[] { "-yjm", "1g", "-ytm", "2g" };
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli();
		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(args, false);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(1024));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(2048));
	}

	/**
	 * Tests the specifying heap memory with old config key for job manager and task manager.
	 */
	@Test
	public void testHeapMemoryPropertyWithOldConfigKey() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB, 2048);
		configuration.setInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB, 4096);

		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli(configuration);

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[0], false);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(2048));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(4096));
	}

	/**
	 * Tests the specifying job manager total process memory with config default value for job manager and task manager.
	 */
	@Test
	public void testJobManagerMemoryPropertyWithConfigDefaultValue() throws Exception {
		int procMemory = 2048;
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCliWithJmAndTmTotalMemory(procMemory);

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[0], false);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(procMemory));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(procMemory));
	}

	@Test
	public void testMultipleYarnShipOptions() throws Exception {
		final String[] args = new String[]{"run", "--yarnship", tmp.newFolder().getAbsolutePath(), "--yarnship", tmp.newFolder().getAbsolutePath()};
		final FlinkYarnSessionCli flinkYarnSessionCli = createFlinkYarnSessionCli();

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(args, false);

		final Configuration executorConfig = flinkYarnSessionCli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<ApplicationId> clientFactory = getClusterClientFactory(executorConfig);
		YarnClusterDescriptor flinkYarnDescriptor = (YarnClusterDescriptor) clientFactory.createClusterDescriptor(executorConfig);

		assertEquals(2, flinkYarnDescriptor.getShipFiles().size());

	}

	///////////
	// Utils //
	///////////

	private ClusterClientFactory<ApplicationId> getClusterClientFactory(final Configuration executorConfig) {
		final ClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
		return clusterClientServiceLoader.getClusterClientFactory(executorConfig);
	}

	private File writeYarnPropertiesFile(String contents) throws IOException {
		File tmpFolder = tmp.newFolder();
		String currentUser = System.getProperty("user.name");

		// copy .yarn-properties-<username>
		File testPropertiesFile = new File(tmpFolder, ".yarn-properties-" + currentUser);
		Files.write(testPropertiesFile.toPath(), contents.getBytes(), StandardOpenOption.CREATE);

		return tmpFolder.getAbsoluteFile();
	}

	private FlinkYarnSessionCli createFlinkYarnSessionCli() throws FlinkException {
		return createFlinkYarnSessionCli(new Configuration());
	}

	private FlinkYarnSessionCli createFlinkYarnSessionCliWithJmAndTmTotalMemory(int totalMemomory) throws FlinkException {
		Configuration configuration = new Configuration();
		configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(totalMemomory));
		configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(totalMemomory));
		return createFlinkYarnSessionCli(configuration);
	}

	private FlinkYarnSessionCli createFlinkYarnSessionCli(Configuration configuration) throws FlinkException {
		return new FlinkYarnSessionCli(
			configuration,
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");
	}
}

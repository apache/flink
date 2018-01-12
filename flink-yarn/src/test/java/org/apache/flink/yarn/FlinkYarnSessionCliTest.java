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

import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import static org.junit.Assert.assertEquals;
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
		CommandLine cmd = parser.parse(options, new String[]{"run", "-j", "fake.jar", "-n", "15",
				"-D", "akka.ask.timeout=5 min", "-D", "env.java.opts=-DappName=foobar"});

		AbstractYarnClusterDescriptor flinkYarnDescriptor = cli.createClusterDescriptor(cmd);

		Assert.assertNotNull(flinkYarnDescriptor);

		Map<String, String> dynProperties =
			FlinkYarnSessionCli.getDynamicProperties(flinkYarnDescriptor.getDynamicPropertiesEncoded());
		assertEquals(2, dynProperties.size());
		assertEquals("5 min", dynProperties.get("akka.ask.timeout"));
		assertEquals("-DappName=foobar", dynProperties.get("env.java.opts"));
	}

	@Test
	public void testNotEnoughTaskSlots() throws Exception {
		String[] params =
			new String[] {"-yn", "2", "-ys", "3", "-p", "7"};

		FlinkYarnSessionCli yarnCLI = new FlinkYarnSessionCli(
			new Configuration(),
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");

		Options options = new Options();
		// TODO: Nasty workaround: We should get rid of the YarnCLI and run options coupling
		options.addOption(CliFrontendParser.PARALLELISM_OPTION);
		yarnCLI.addGeneralOptions(options);
		yarnCLI.addRunOptions(options);

		final CommandLine commandLine = CliFrontendParser.parse(options, params, true);

		ClusterSpecification clusterSpecification = yarnCLI.getClusterSpecification(commandLine);

		// each task manager has 3 slots but the parallelism is 7. Thus the slots should be increased.
		assertEquals(4, clusterSpecification.getSlotsPerTaskManager());
		assertEquals(2, clusterSpecification.getNumberTaskManagers());
	}

	@Test
	public void testCorrectSettingOfMaxSlots() throws Exception {
		String[] params =
			new String[] {"-yn", "2", "-ys", "3"};

		FlinkYarnSessionCli yarnCLI = new FlinkYarnSessionCli(
			new Configuration(),
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");

		final CommandLine commandLine = yarnCLI.parseCommandLineOptions(params, true);

		AbstractYarnClusterDescriptor descriptor = yarnCLI.createClusterDescriptor(commandLine);

		final ClusterSpecification clusterSpecification = yarnCLI.getClusterSpecification(commandLine);

		// each task manager has 3 slots but the parallelism is 7. Thus the slots should be increased.
		assertEquals(3, clusterSpecification.getSlotsPerTaskManager());
		assertEquals(2, clusterSpecification.getNumberTaskManagers());
	}

	@Test
	public void testZookeeperNamespaceProperty() throws Exception {
		String zkNamespaceCliInput = "flink_test_namespace";

		String[] params = new String[] {"-yn", "2", "-yz", zkNamespaceCliInput};

		FlinkYarnSessionCli yarnCLI = new FlinkYarnSessionCli(
			new Configuration(),
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");

		CommandLine commandLine = yarnCLI.parseCommandLineOptions(params, true);

		AbstractYarnClusterDescriptor descriptor = yarnCLI.createClusterDescriptor(commandLine);

		assertEquals(zkNamespaceCliInput, descriptor.getZookeeperNamespace());
	}

	/**
	 * Test that the CliFrontend is able to pick up the .yarn-properties file from a specified location.
	 */
	@Test
	public void testResumeFromYarnPropertiesFile() throws Exception {

		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		final Configuration configuration = new Configuration();
		configuration.setString(YarnConfigOptions.PROPERTIES_FILE_LOCATION, directoryPath.getAbsolutePath());

		final FlinkYarnSessionCli flinkYarnSessionCli = new FlinkYarnSessionCli(
			configuration,
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {}, true);

		final String clusterId = flinkYarnSessionCli.getClusterId(commandLine);

		assertEquals(TEST_YARN_APPLICATION_ID.toString(), clusterId);
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

		new FlinkYarnSessionCli(
			configuration,
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");
	}

	@Test
	public void testResumeFromYarnID() throws Exception {
		final Configuration configuration = new Configuration();
		final FlinkYarnSessionCli flinkYarnSessionCli = new FlinkYarnSessionCli(
			configuration,
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString()}, true);

		final String clusterId = flinkYarnSessionCli.getClusterId(commandLine);

		assertEquals(TEST_YARN_APPLICATION_ID.toString(), clusterId);
	}

	@Test
	public void testResumeFromYarnIDZookeeperNamespace() throws Exception {
		final Configuration configuration = new Configuration();
		final FlinkYarnSessionCli flinkYarnSessionCli = new FlinkYarnSessionCli(
			configuration,
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString()}, true);

		final AbstractYarnClusterDescriptor clusterDescriptor = flinkYarnSessionCli.createClusterDescriptor(commandLine);

		final Configuration clusterDescriptorConfiguration = clusterDescriptor.getFlinkConfiguration();

		String zkNs = clusterDescriptorConfiguration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);
		assertTrue(zkNs.matches("application_\\d+_0042"));
	}

	@Test
	public void testResumeFromYarnIDZookeeperNamespaceOverride() throws Exception {
		final Configuration configuration = new Configuration();
		final FlinkYarnSessionCli flinkYarnSessionCli = new FlinkYarnSessionCli(
			configuration,
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");

		final String overrideZkNamespace = "my_cluster";

		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {"-yid", TEST_YARN_APPLICATION_ID.toString(), "-yz", overrideZkNamespace}, true);

		final AbstractYarnClusterDescriptor clusterDescriptor = flinkYarnSessionCli.createClusterDescriptor(commandLine);

		final Configuration clusterDescriptorConfiguration = clusterDescriptor.getFlinkConfiguration();

		final String clusterId = clusterDescriptorConfiguration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);
		assertEquals(overrideZkNamespace, clusterId);
	}

	@Test
	public void testYarnIDOverridesPropertiesFile() throws Exception {
		File directoryPath = writeYarnPropertiesFile(validPropertiesFile);

		final Configuration configuration = new Configuration();
		configuration.setString(YarnConfigOptions.PROPERTIES_FILE_LOCATION, directoryPath.getAbsolutePath());

		final FlinkYarnSessionCli flinkYarnSessionCli = new FlinkYarnSessionCli(
			configuration,
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");
		final CommandLine commandLine = flinkYarnSessionCli.parseCommandLineOptions(new String[] {"-yid", TEST_YARN_APPLICATION_ID_2.toString() }, true);
		final String clusterId = flinkYarnSessionCli.getClusterId(commandLine);
		assertEquals(TEST_YARN_APPLICATION_ID_2.toString(), clusterId);
	}

	///////////
	// Utils //
	///////////

	private File writeYarnPropertiesFile(String contents) throws IOException {
		File tmpFolder = tmp.newFolder();
		String currentUser = System.getProperty("user.name");

		// copy .yarn-properties-<username>
		File testPropertiesFile = new File(tmpFolder, ".yarn-properties-" + currentUser);
		Files.write(testPropertiesFile.toPath(), contents.getBytes(), StandardOpenOption.CREATE);

		return tmpFolder.getAbsoluteFile();
	}
}

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

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.runtime.jobmanager.JobManagerProcessUtils.createDefaultJobManagerProcessSpec;
import static org.apache.flink.yarn.configuration.YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link YarnClusterDescriptor}.
 */
public class YarnClusterDescriptorTest extends TestLogger {

	private static final int YARN_MAX_VCORES = 16;
	private static YarnConfiguration yarnConfiguration;

	private static YarnClient yarnClient;

	private final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
		.setSlotsPerTaskManager(Integer.MAX_VALUE)
		.createClusterSpecification();
	private final ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[0], null);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File flinkJar;

	@BeforeClass
	public static void setupClass() {
		yarnConfiguration = new YarnConfiguration();
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(yarnConfiguration);
		yarnClient.start();
	}

	@Before
	public void beforeTest() throws IOException {
		temporaryFolder.create();
		flinkJar = temporaryFolder.newFile("flink.jar");
	}

	@AfterClass
	public static void tearDownClass() {
		yarnClient.stop();
	}

	@Test
	public void testFailIfTaskSlotsHigherThanMaxVcores() throws ClusterDeploymentException {
		final Configuration flinkConfiguration = new Configuration();

		YarnClusterDescriptor clusterDescriptor = createYarnClusterDescriptor(flinkConfiguration);

		clusterDescriptor.setLocalJarPath(new Path(flinkJar.getPath()));

		try {
			clusterDescriptor.deploySessionCluster(clusterSpecification);

			fail("The deploy call should have failed.");
		} catch (ClusterDeploymentException e) {
			// we expect the cause to be an IllegalConfigurationException
			if (!(e.getCause() instanceof IllegalConfigurationException)) {
				throw e;
			}
		} finally {
			clusterDescriptor.close();
		}
	}

	@Test
	public void testConfigOverwrite() throws ClusterDeploymentException {
		Configuration configuration = new Configuration();
		// overwrite vcores in config
		configuration.setInteger(YarnConfigOptions.VCORES, Integer.MAX_VALUE);

		YarnClusterDescriptor clusterDescriptor = createYarnClusterDescriptor(configuration);

		clusterDescriptor.setLocalJarPath(new Path(flinkJar.getPath()));

		// configure slots
		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.createClusterSpecification();

		try {
			clusterDescriptor.deploySessionCluster(clusterSpecification);

			fail("The deploy call should have failed.");
		} catch (ClusterDeploymentException e) {
			// we expect the cause to be an IllegalConfigurationException
			if (!(e.getCause() instanceof IllegalConfigurationException)) {
				throw e;
			}
		} finally {
			clusterDescriptor.close();
		}
	}

	@Test
	public void testSetupApplicationMasterContainer() {
		Configuration cfg = new Configuration();
		YarnClusterDescriptor clusterDescriptor = createYarnClusterDescriptor(cfg);

		final JobManagerProcessSpec jobManagerProcessSpec = createDefaultJobManagerProcessSpec(1024);
		final String java = "$JAVA_HOME/bin/java";
		final String jvmmem = JobManagerProcessUtils.generateJvmParametersStr(jobManagerProcessSpec, cfg);
		final String dynamicParameters = JobManagerProcessUtils.generateDynamicConfigsStr(jobManagerProcessSpec);
		final String jvmOpts = "-Djvm"; // if set
		final String jmJvmOpts = "-DjmJvm"; // if set
		final String krb5 = "-Djava.security.krb5.conf=krb5.conf";
		final String logfile =
			"-Dlog.file=\"" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
				"/jobmanager.log\""; // if set
		final String logback =
			"-Dlogback.configurationFile=file:" + YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME; // if set
		final String log4j =
			"-Dlog4j.configuration=file:" + YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME +
				" -Dlog4j.configurationFile=file:" + YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME; // if set
		final String mainClass = clusterDescriptor.getYarnSessionClusterEntrypoint();
		final String redirects =
			"1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.out " +
			"2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.err";

		try {
			// no logging, with/out krb5
			assertEquals(
				java + " " + jvmmem +
					"" + // jvmOpts
					"" + // logging
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						false,
						jobManagerProcessSpec)
					.getCommands().get(0));

			assertEquals(
				java + " " + jvmmem +
					" " + krb5 + // jvmOpts
					"" + // logging
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						true,
						jobManagerProcessSpec)
					.getCommands().get(0));

			// logback only, with/out krb5
			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
			assertEquals(
				java + " " + jvmmem +
					"" + // jvmOpts
					" " + logfile + " " + logback +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						false,
						jobManagerProcessSpec)
					.getCommands().get(0));

			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
			assertEquals(
				java + " " + jvmmem +
					" " + krb5 + // jvmOpts
					" " + logfile + " " + logback +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						true,
						jobManagerProcessSpec)
					.getCommands().get(0));

			// log4j, with/out krb5
			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
			assertEquals(
				java + " " + jvmmem +
					"" + // jvmOpts
					" " + logfile + " " + log4j +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						false,
						jobManagerProcessSpec)
					.getCommands().get(0));

			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
			assertEquals(
				java + " " + jvmmem +
					" " + krb5 + // jvmOpts
					" " + logfile + " " + log4j +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						true,
						jobManagerProcessSpec)
					.getCommands().get(0));

			// logback, with/out krb5
			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
			assertEquals(
				java + " " + jvmmem +
					"" + // jvmOpts
					" " + logfile + " " + logback +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						false,
						jobManagerProcessSpec)
					.getCommands().get(0));

			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
			assertEquals(
				java + " " + jvmmem +
					" " + krb5 + // jvmOpts
					" " + logfile + " " + logback +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						true,
						jobManagerProcessSpec)
					.getCommands().get(0));

			// logback, with/out krb5, different JVM opts
			// IMPORTANT: Be aware that we are using side effects here to modify the created YarnClusterDescriptor,
			// because we have a reference to the ClusterDescriptor's configuration which we modify continuously
			cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
			assertEquals(
				java + " " + jvmmem +
					" " + jvmOpts +
					" " + logfile + " " + logback +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						false,
						jobManagerProcessSpec)
					.getCommands().get(0));

			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
			assertEquals(
				java + " " + jvmmem +
					" " + jvmOpts + " " + krb5 + // jvmOpts
					" " + logfile + " " + logback +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						true,
						jobManagerProcessSpec)
					.getCommands().get(0));

			// log4j, with/out krb5, different JVM opts
			// IMPORTANT: Be aware that we are using side effects here to modify the created YarnClusterDescriptor
			cfg.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);
			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
			assertEquals(
				java + " " + jvmmem +
					" " + jvmOpts + " " + jmJvmOpts +
					" " + logfile + " " + log4j +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						false,
						jobManagerProcessSpec)
					.getCommands().get(0));

			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
			assertEquals(
				java + " " + jvmmem +
					" " + jvmOpts + " " + jmJvmOpts + " " + krb5 + // jvmOpts
					" " + logfile + " " + log4j +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						true,
						jobManagerProcessSpec)
					.getCommands().get(0));

			// now try some configurations with different yarn.container-start-command-template
			// IMPORTANT: Be aware that we are using side effects here to modify the created YarnClusterDescriptor
			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
			cfg.setString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
				"%java% 1 %jvmmem% 2 %jvmopts% 3 %logging% 4 %class% 5 %args% 6 %redirects%");
			assertEquals(
				java + " 1 " + jvmmem +
					" 2 " + jvmOpts + " " + jmJvmOpts + " " + krb5 + // jvmOpts
					" 3 " + logfile + " " + logback +
					" 4 " + mainClass + " 5 " + dynamicParameters + " 6 " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						true,
						jobManagerProcessSpec)
					.getCommands().get(0));

			cfg.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
			cfg.setString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
				"%java% %logging% %jvmopts% %jvmmem% %class% %args% %redirects%");
			// IMPORTANT: Be aware that we are using side effects here to modify the created YarnClusterDescriptor
			assertEquals(
				java +
					" " + logfile + " " + logback +
					" " + jvmOpts + " " + jmJvmOpts + " " + krb5 + // jvmOpts
					" " + jvmmem +
					" " + mainClass + " " + dynamicParameters + " " + redirects,
				clusterDescriptor
					.setupApplicationMasterContainer(
						mainClass,
						true,
						jobManagerProcessSpec)
					.getCommands().get(0));
		} finally {
			clusterDescriptor.close();
		}
	}

	/**
	 * Tests to ship files through the {@code YarnClusterDescriptor.addShipFiles}.
	 */
	@Test
	public void testExplicitFileShipping() throws Exception {
		try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
			descriptor.setLocalJarPath(new Path("/path/to/flink.jar"));

			File libFile = temporaryFolder.newFile("libFile.jar");
			File libFolder = temporaryFolder.newFolder().getAbsoluteFile();

			Assert.assertFalse(descriptor.getShipFiles().contains(libFile));
			Assert.assertFalse(descriptor.getShipFiles().contains(libFolder));

			List<File> shipFiles = new ArrayList<>();
			shipFiles.add(libFile);
			shipFiles.add(libFolder);

			descriptor.addShipFiles(shipFiles);

			Assert.assertTrue(descriptor.getShipFiles().contains(libFile));
			Assert.assertTrue(descriptor.getShipFiles().contains(libFolder));

			// only execute part of the deployment to test for shipped files
			Set<File> effectiveShipFiles = new HashSet<>();
			descriptor.addLibFoldersToShipFiles(effectiveShipFiles);

			Assert.assertEquals(0, effectiveShipFiles.size());
			Assert.assertEquals(2, descriptor.getShipFiles().size());
			Assert.assertTrue(descriptor.getShipFiles().contains(libFile));
			Assert.assertTrue(descriptor.getShipFiles().contains(libFolder));
		}
	}

	@Test
	public void testEnvironmentLibShipping() throws Exception {
		testEnvironmentDirectoryShipping(ConfigConstants.ENV_FLINK_LIB_DIR, false);
	}

	@Test
	public void testEnvironmentPluginsShipping() throws Exception {
		testEnvironmentDirectoryShipping(ConfigConstants.ENV_FLINK_PLUGINS_DIR, true);
	}

	private void testEnvironmentDirectoryShipping(String environmentVariable, boolean onlyShip) throws Exception {
		try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
			File libFolder = temporaryFolder.newFolder().getAbsoluteFile();
			File libFile = new File(libFolder, "libFile.jar");
			assertTrue(libFile.createNewFile());

			Set<File> effectiveShipFiles = new HashSet<>();

			final Map<String, String> oldEnv = System.getenv();
			try {
				Map<String, String> env = new HashMap<>(1);
				env.put(environmentVariable, libFolder.getAbsolutePath());
				CommonTestUtils.setEnv(env);
				// only execute part of the deployment to test for shipped files
				if (onlyShip) {
					descriptor.addPluginsFoldersToShipFiles(effectiveShipFiles);
				} else {
					descriptor.addLibFoldersToShipFiles(effectiveShipFiles);
				}
			} finally {
				CommonTestUtils.setEnv(oldEnv);
			}

			// only add the ship the folder, not the contents
			Assert.assertFalse(effectiveShipFiles.contains(libFile));
			Assert.assertTrue(effectiveShipFiles.contains(libFolder));
			Assert.assertFalse(descriptor.getShipFiles().contains(libFile));
			Assert.assertFalse(descriptor.getShipFiles().contains(libFolder));
		}
	}

	@Test
	public void testEnvironmentEmptyPluginsShipping() {
		try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
			File pluginsFolder = Paths.get(temporaryFolder.getRoot().getAbsolutePath(), "s0m3_p4th_th4t_sh0uld_n0t_3x1sts").toFile();
			Set<File> effectiveShipFiles = new HashSet<>();

			final Map<String, String> oldEnv = System.getenv();
			try {
				Map<String, String> env = new HashMap<>(1);
				env.put(ConfigConstants.ENV_FLINK_PLUGINS_DIR, pluginsFolder.getAbsolutePath());
				CommonTestUtils.setEnv(env);
				// only execute part of the deployment to test for shipped files
				descriptor.addPluginsFoldersToShipFiles(effectiveShipFiles);
			} finally {
				CommonTestUtils.setEnv(oldEnv);
			}

			assertTrue(effectiveShipFiles.isEmpty());
		}
	}

	@Test
	public void testDisableSystemClassPathIncludeUserJarAndWithIllegalShipDirectoryName() throws IOException {
		final Configuration configuration = new Configuration();
		configuration.setString(CLASSPATH_INCLUDE_USER_JAR, YarnConfigOptions.UserJarInclusion.DISABLED.toString());

		final YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor(configuration);
		try {
			yarnClusterDescriptor.addShipFiles(Collections.singletonList(temporaryFolder.newFolder(ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR)));
			fail();
		} catch (IllegalArgumentException exception) {
			assertThat(exception.getMessage(), containsString("This is an illegal ship directory :"));
		}
	}

	/**
	 * Tests that the YarnClient is only shut down if it is not shared.
	 */
	@Test
	public void testYarnClientShutDown() {
		YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor();

		yarnClusterDescriptor.close();

		assertTrue(yarnClient.isInState(Service.STATE.STARTED));

		final YarnClient closableYarnClient = YarnClient.createYarnClient();
		closableYarnClient.init(yarnConfiguration);
		closableYarnClient.start();

		yarnClusterDescriptor = YarnTestUtils.createClusterDescriptorWithLogging(
				temporaryFolder.getRoot().getAbsolutePath(),
				new Configuration(),
				yarnConfiguration,
				closableYarnClient,
				false);

		yarnClusterDescriptor.close();

		assertTrue(closableYarnClient.isInState(Service.STATE.STOPPED));
	}

	@Test
	public void testDeployApplicationClusterWithDeploymentTargetNotCorrectlySet() {
		final Configuration flinkConfig = new Configuration();
		flinkConfig.set(PipelineOptions.JARS, Collections.singletonList("file:///path/of/user.jar"));
		flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName());
		try (final YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor(flinkConfig)) {
			assertThrows(
				"Expected deployment.target=yarn-application",
				ClusterDeploymentException.class,
				() -> yarnClusterDescriptor.deployApplicationCluster(clusterSpecification, appConfig));
		}
	}

	@Test
	public void testDeployApplicationClusterWithMultipleJarsSet() {
		final Configuration flinkConfig = new Configuration();
		flinkConfig.set(PipelineOptions.JARS, Arrays.asList("local:///path/of/user.jar", "local:///user2.jar"));
		flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
		try (final YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor(flinkConfig)) {
			assertThrows(
				"Should only have one jar",
				IllegalArgumentException.class,
				() -> yarnClusterDescriptor.deployApplicationCluster(clusterSpecification, appConfig));
		}
	}

	private YarnClusterDescriptor createYarnClusterDescriptor() {
		return createYarnClusterDescriptor(new Configuration());
	}

	private YarnClusterDescriptor createYarnClusterDescriptor(Configuration configuration) {
		YarnTestUtils.configureLogFile(configuration, temporaryFolder.getRoot().getAbsolutePath());

		return YarnClusterDescriptorBuilder.newBuilder(yarnClient, true)
			.setFlinkConfiguration(configuration)
			.setYarnConfiguration(yarnConfiguration)
			.setYarnClusterInformationRetriever(() -> YARN_MAX_VCORES)
			.build();

	}
}

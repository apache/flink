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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class YarnClusterDescriptorTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File flinkJar;
	private File flinkConf;

	@Before
	public void beforeTest() throws IOException {
		temporaryFolder.create();
		flinkJar = temporaryFolder.newFile("flink.jar");
		flinkConf = temporaryFolder.newFile("flink-conf.yaml");
	}

	@Test
	public void testFailIfTaskSlotsHigherThanMaxVcores() {

		YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor();

		clusterDescriptor.setLocalJarPath(new Path(flinkJar.getPath()));
		clusterDescriptor.setFlinkConfiguration(new Configuration());
		clusterDescriptor.setConfigurationDirectory(temporaryFolder.getRoot().getAbsolutePath());
		clusterDescriptor.setConfigurationFilePath(new Path(flinkConf.getPath()));

		// configure slots too high
		clusterDescriptor.setTaskManagerSlots(Integer.MAX_VALUE);

		try {
			clusterDescriptor.deploy();

			fail("The deploy call should have failed.");
		} catch (RuntimeException e) {
			// we expect the cause to be an IllegalConfigurationException
			if (!(e.getCause() instanceof IllegalConfigurationException)) {
				throw e;
			}
		}
	}

	@Test
	public void testConfigOverwrite() {

		YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor();

		Configuration configuration = new Configuration();
		// overwrite vcores in config
		configuration.setInteger(ConfigConstants.YARN_VCORES, Integer.MAX_VALUE);

		clusterDescriptor.setLocalJarPath(new Path(flinkJar.getPath()));
		clusterDescriptor.setFlinkConfiguration(configuration);
		clusterDescriptor.setConfigurationDirectory(temporaryFolder.getRoot().getAbsolutePath());
		clusterDescriptor.setConfigurationFilePath(new Path(flinkConf.getPath()));

		// configure slots
		clusterDescriptor.setTaskManagerSlots(1);

		try {
			clusterDescriptor.deploy();

			fail("The deploy call should have failed.");
		} catch (RuntimeException e) {
			// we expect the cause to be an IllegalConfigurationException
			if (!(e.getCause() instanceof IllegalConfigurationException)) {
				throw e;
			}
		}
	}

	@Test
	public void testSetupApplicationMasterContainer() {
		YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor();
		final Configuration cfg = new Configuration();
		clusterDescriptor.setFlinkConfiguration(cfg);

		final String java = "$JAVA_HOME/bin/java";
		final String jvmmem = "-Xmx424m";
		final String jvmOpts = "-Djvm"; // if set
		final String jmJvmOpts = "-DjmJvm"; // if set
		final String krb5 = "-Djava.security.krb5.conf=krb5.conf";
		final String logfile =
			"-Dlog.file=\"" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
				"/jobmanager.log\""; // if set
		final String logback =
			"-Dlogback.configurationFile=file:" + FlinkYarnSessionCli.CONFIG_FILE_LOGBACK_NAME; // if set
		final String log4j =
			"-Dlog4j.configuration=file:" + FlinkYarnSessionCli.CONFIG_FILE_LOG4J_NAME; // if set
		final String mainClass = clusterDescriptor.getApplicationMasterClass().getName();
		final String args = "";
		final String redirects =
			"1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.out " +
			"2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.err";

		// no logging, with/out krb5
		assertEquals(
			java + " " + jvmmem +
				" " + // jvmOpts
				" " + // logging
				" " + mainClass + " " + args + " " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(false, false, false)
				.getCommands().get(0));

		assertEquals(
			java + " " + jvmmem +
				" " + " " + krb5 +// jvmOpts
				" " + // logging
				" " + mainClass + " " + args + " " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(false, false, true)
				.getCommands().get(0));

		// logback only, with/out krb5
		assertEquals(
			java + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + logback +
				" " + mainClass + " " + args + " " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, false, false)
				.getCommands().get(0));

		assertEquals(
			java + " " + jvmmem +
				" " + " " + krb5 +// jvmOpts
				" " + logfile + " " + logback +
				" " + mainClass + " " + args + " " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, false, true)
				.getCommands().get(0));

		// log4j, with/out krb5
		assertEquals(
			java + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(false, true, false)
				.getCommands().get(0));

		assertEquals(
			java + " " + jvmmem +
				" " + " " + krb5 +// jvmOpts
				" " + logfile + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(false, true, true)
				.getCommands().get(0));

		// logback + log4j, with/out krb5
		assertEquals(
			java + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, true, false)
				.getCommands().get(0));

		assertEquals(
			java + " " + jvmmem +
				" " + " " + krb5 +// jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, true, true)
				.getCommands().get(0));

		// logback + log4j, with/out krb5, different JVM opts
		cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		assertEquals(
			java + " " + jvmmem +
				" " + jvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " "  + args + " "+ redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, true, false)
				.getCommands().get(0));

		assertEquals(
			java + " " + jvmmem +
				" " + jvmOpts + " " + krb5 +// jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " "  + args + " "+ redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, true, true)
				.getCommands().get(0));

		// logback + log4j, with/out krb5, different JVM opts
		cfg.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);
		assertEquals(
			java + " " + jvmmem +
				" " + jvmOpts + " " + jmJvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " "  + args + " "+ redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, true, false)
				.getCommands().get(0));

		assertEquals(
			java + " " + jvmmem +
				" " + jvmOpts + " " + jmJvmOpts + " " + krb5 +// jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " "  + args + " "+ redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, true, true)
				.getCommands().get(0));

		// now try some configurations with different yarn.container-start-command-template

		cfg.setString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
			"%java% 1 %jvmmem% 2 %jvmopts% 3 %logging% 4 %class% 5 %args% 6 %redirects%");
		assertEquals(
			java + " 1 " + jvmmem +
				" 2 " + jvmOpts + " " + jmJvmOpts + " " + krb5 + // jvmOpts
				" 3 " + logfile + " " + logback + " " + log4j +
				" 4 " + mainClass + " 5 " + args + " 6 " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, true, true)
				.getCommands().get(0));

		cfg.setString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
			"%java% %logging% %jvmopts% %jvmmem% %class% %args% %redirects%");
		assertEquals(
			java +
				" " + logfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + jmJvmOpts + " " + krb5 + // jvmOpts
				" " + jvmmem +
				" " + mainClass + " " + args + " " + redirects,
			clusterDescriptor
				.setupApplicationMasterContainer(true, true, true)
				.getCommands().get(0));
	}
}

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

package org.apache.flink.test.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.test.util.ShellScript;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for passing and loading dynamical properties of task manager.
 * Usually(in Yarn, Kubernetes), the taskmanager java start commands are wrapped within bash. This test will generate a
 * launch_container.sh script to validate the dynamical properties passing and loading.
 */
public class TaskManagerLoadingDynamicPropertiesITCase extends TestLogger {

	private static final String KEY_A = "key.a";
	private static final String VALUE_A = "#a,b&c^d*e@f(g!h";
	private static final String KEY_B = "key.b";
	private static final String VALUE_B = "'foobar";
	private static final String KEY_C = "key.c";
	private static final String VALUE_C = "foo''bar";
	private static final String KEY_D = "key.d";
	private static final String VALUE_D = "'foo' 'bar'";
	private static final String KEY_E = "key.e";
	private static final String VALUE_E = "foo\"bar'";
	private static final String KEY_F = "key.f";
	private static final String VALUE_F = "\"foo\" \"bar\"";

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testLoadingDynamicPropertiesInBash() throws Exception {
		final Configuration clientConfiguration = new Configuration();
		final File root = folder.getRoot();
		final File homeDir = new File(root, "home");
		assertTrue(homeDir.mkdir());
		BootstrapTools.writeConfiguration(clientConfiguration, new File(homeDir, FLINK_CONF_FILENAME));

		final Configuration jmUpdatedConfiguration = getJobManagerUpdatedConfiguration();

		final File shellScriptFile = generateLaunchContainerScript(
			homeDir,
			BootstrapTools.getDynamicPropertiesAsString(clientConfiguration, jmUpdatedConfiguration));

		Process process = new ProcessBuilder(shellScriptFile.getAbsolutePath()).start();
		try {
			final StringWriter processOutput = new StringWriter();
			new CommonTestUtils.PipeForwarder(process.getErrorStream(), processOutput);

			if (!process.waitFor(10, TimeUnit.SECONDS)) {
				throw new Exception("TestingTaskManagerRunner did not shutdown in time.");
			}
			assertEquals(processOutput.toString(), 0, process.exitValue());
		} finally {
			process.destroy();
		}
	}

	private Configuration getJobManagerUpdatedConfiguration() {
		final Configuration updatedConfig = new Configuration();
		updatedConfig.setString(KEY_A, VALUE_A);
		updatedConfig.setString(KEY_B, VALUE_B);
		updatedConfig.setString(KEY_C, VALUE_C);
		updatedConfig.setString(KEY_D, VALUE_D);
		updatedConfig.setString(KEY_E, VALUE_E);
		updatedConfig.setString(KEY_F, VALUE_F);
		return updatedConfig;
	}

	private File generateLaunchContainerScript(File homeDir, String dynamicProperties) throws IOException {
		final ShellScript.ShellScriptBuilder shellScriptBuilder = ShellScript.createShellScriptBuilder();
		final List<String> commands = new ArrayList<>();
		commands.add(getJavaCommandWithOS());
		commands.add(getInternalClassNameWithOS(TestingTaskManagerRunner.class.getName()));
		commands.add("--configDir");
		commands.add(homeDir.getAbsolutePath());
		commands.add(dynamicProperties);
		shellScriptBuilder.env("CLASSPATH", getCurrentClasspath());
		shellScriptBuilder.command(commands);
		final File shellScriptFile = new File(homeDir, "launch_container" + ShellScript.getScriptExtension());
		shellScriptBuilder.write(shellScriptFile);
		return shellScriptFile;
	}

	private String getJavaCommandWithOS() {
		if (OperatingSystem.isWindows()) {
			return "\"" + getJavaCommandPath() + "\"";
		}
		return getJavaCommandPath();
	}

	private String getInternalClassNameWithOS(String className) {
		if (!OperatingSystem.isWindows()) {
			return className.replace("$", "'$'");
		}
		return className;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * The testing taskmanager runner. The exit code will be 0 if configuration values check passed.
	 */
	public static class TestingTaskManagerRunner {

		public static void main(String[] args) throws FlinkParseException {
			final Configuration flinkConfig = TaskManagerRunner.loadConfiguration(args);
			assertThat(
				flinkConfig.toMap().values(),
				Matchers.containsInAnyOrder(VALUE_A, VALUE_B, VALUE_C, VALUE_D, VALUE_E, VALUE_F));
		}
	}
}

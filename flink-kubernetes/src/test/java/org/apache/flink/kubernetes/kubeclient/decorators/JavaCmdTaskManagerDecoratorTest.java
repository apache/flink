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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;
import org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

import io.fabric8.kubernetes.api.model.Container;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.NATIVE_KUBERNETES_COMMAND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * General tests for the{@link JavaCmdTaskManagerDecorator}.
 */
public class JavaCmdTaskManagerDecoratorTest extends KubernetesTaskManagerTestBase {

	private static final String KUBERNETES_ENTRY_PATH = "/opt/flink/bin/start.sh";
	private static final String FLINK_CONF_DIR_IN_POD = "/opt/flink/flink-conf-";
	private static final String FLINK_LOG_DIR_IN_POD = "/opt/flink/flink-log-";

	private static final String java = "$JAVA_HOME/bin/java";
	private static final String classpath = "-classpath $FLINK_CLASSPATH";
	private static final String jvmOpts = "-Djvm";

	private static final String tmJvmMem =
			"-Xmx251658235 -Xms251658235 -XX:MaxDirectMemorySize=211392922 -XX:MaxMetaspaceSize=268435456";

	private static final String mainClass = KubernetesTaskExecutorRunner.class.getCanonicalName();
	private String mainClassArgs;

	// Logging variables
	private static final String logback =
			String.format("-Dlogback.configurationFile=file:%s/%s", FLINK_CONF_DIR_IN_POD, CONFIG_FILE_LOGBACK_NAME);
	private static final String log4j = String.format(
		"-Dlog4j.configuration=file:%s/%s -Dlog4j.configurationFile=file:%s/%s",
		FLINK_CONF_DIR_IN_POD,
		CONFIG_FILE_LOG4J_NAME,
		FLINK_CONF_DIR_IN_POD,
		CONFIG_FILE_LOG4J_NAME);
	private static final String tmLogfile =
			String.format("-Dlog.file=%s/taskmanager.log", FLINK_LOG_DIR_IN_POD);

	private JavaCmdTaskManagerDecorator javaCmdTaskManagerDecorator;

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		flinkConfig.setString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, KUBERNETES_ENTRY_PATH);
		flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, FLINK_CONF_DIR_IN_POD);
		flinkConfig.set(KubernetesConfigOptions.FLINK_LOG_DIR, FLINK_LOG_DIR_IN_POD);
	}

	@Override
	public void onSetup() throws Exception {
		super.onSetup();

		this.mainClassArgs = String.format(
				"%s --configDir %s",
				TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec),
			FLINK_CONF_DIR_IN_POD);

		this.javaCmdTaskManagerDecorator = new JavaCmdTaskManagerDecorator(this.kubernetesTaskManagerParameters);
	}

	@Test
	public void testWhetherContainerOrPodIsUpdated() {
		final FlinkPod resultFlinkPod = javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
		assertEquals(this.baseFlinkPod.getPod(), resultFlinkPod.getPod());
		assertNotEquals(this.baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
	}

	@Test
	public void testStartCommandWithoutLog4jAndLogback() {
		final Container resultMainContainer =
			javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer();
		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = getTaskManagerExpectedCommand("", "");
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLog4j() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);

		final Container resultMainContainer =
			javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = getTaskManagerExpectedCommand("", log4j);
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		final Container resultMainContainer =
			javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = getTaskManagerExpectedCommand("", logback);
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLog4jAndLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);

		final Container resultMainContainer =
			javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer();
		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = getTaskManagerExpectedCommand("", logback + " " + log4j);
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLogAndJVMOpts() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		flinkConfig.set(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		final Container resultMainContainer =
				javaCmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand =
				getTaskManagerExpectedCommand(jvmOpts, logback + " " + log4j);
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLogAndJMOpts() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		flinkConfig.set(CoreOptions.FLINK_TM_JVM_OPTIONS, jvmOpts);
		final Container resultMainContainer =
				javaCmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());
		final String expectedCommand = getTaskManagerExpectedCommand(jvmOpts, logback + " " + log4j);
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testContainerStartCommandTemplate1() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		final String containerStartCommandTemplate =
				"%java% 1 %classpath% 2 %jvmmem% %jvmopts% %logging% %class% %args%";
		this.flinkConfig.set(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
				containerStartCommandTemplate);

		final String tmJvmOpts = "-DjmJvm";
		this.flinkConfig.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		this.flinkConfig.setString(CoreOptions.FLINK_TM_JVM_OPTIONS, tmJvmOpts);

		final Container resultMainContainer =
				javaCmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();
		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = java + " 1 " + classpath + " 2 " + tmJvmMem +
				" " + jvmOpts + " " + tmJvmOpts +
				" " + tmLogfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs;
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testContainerStartCommandTemplate2() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		final String containerStartCommandTemplate =
				"%java% %jvmmem% %logging% %jvmopts% %class% %args%";
		this.flinkConfig.set(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
				containerStartCommandTemplate);

		final String tmJvmOpts = "-DjmJvm";
		this.flinkConfig.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		this.flinkConfig.setString(CoreOptions.FLINK_TM_JVM_OPTIONS, tmJvmOpts);

		final Container resultMainContainer =
				javaCmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = java + " " + tmJvmMem +
				" " + tmLogfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + tmJvmOpts + " " + mainClass +
				" " + mainClassArgs;
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	private String getTaskManagerExpectedCommand(String jvmAllOpts, String logging) {
		return java + " " + classpath + " " + tmJvmMem +
				(jvmAllOpts.isEmpty() ? "" : " " + jvmAllOpts) +
				(logging.isEmpty() ? "" : " " + tmLogfile + " " + logging) +
				" " + mainClass + " " +  mainClassArgs;
	}
}

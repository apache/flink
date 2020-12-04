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
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;

import io.fabric8.kubernetes.api.model.Container;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.NATIVE_KUBERNETES_COMMAND;
import static org.apache.flink.runtime.jobmanager.JobManagerProcessUtils.createDefaultJobManagerProcessSpec;
import static org.apache.flink.runtime.jobmanager.JobManagerProcessUtils.generateDynamicConfigsStr;
import static org.apache.flink.runtime.jobmanager.JobManagerProcessUtils.generateJvmParametersStr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * General tests for the {@link JavaCmdJobManagerDecorator}.
 */
public class JavaCmdJobManagerDecoratorTest extends KubernetesJobManagerTestBase {

	private static final String KUBERNETES_ENTRY_PATH = "/opt/bin/start.sh";
	private static final String FLINK_CONF_DIR_IN_POD = "/opt/flink/flink-conf-";
	private static final String FLINK_LOG_DIR_IN_POD = "/opt/flink/flink-log-";
	private static final String ENTRY_POINT_CLASS = KubernetesSessionClusterEntrypoint.class.getCanonicalName();

	private static final String java = "$JAVA_HOME/bin/java";
	private static final String classpath = "-classpath $FLINK_CLASSPATH";
	private static final String jvmOpts = "-Djvm";

	// Logging variables
	private static final String logback =
			String.format("-Dlogback.configurationFile=file:%s/%s", FLINK_CONF_DIR_IN_POD, CONFIG_FILE_LOGBACK_NAME);
	private static final String log4j = String.format(
		"-Dlog4j.configuration=file:%s/%s -Dlog4j.configurationFile=file:%s/%s",
		FLINK_CONF_DIR_IN_POD,
		CONFIG_FILE_LOG4J_NAME,
		FLINK_CONF_DIR_IN_POD,
		CONFIG_FILE_LOG4J_NAME);
	private static final String jmLogfile = String.format("-Dlog.file=%s/jobmanager.log", FLINK_LOG_DIR_IN_POD);

	// Memory variables
	private static final JobManagerProcessSpec JOB_MANAGER_PROCESS_SPEC = createDefaultJobManagerProcessSpec(JOB_MANAGER_MEMORY);

	private final String jmJvmMem = generateJvmParametersStr(JOB_MANAGER_PROCESS_SPEC, flinkConfig);
	private final String jmDynamicProperties = generateDynamicConfigsStr(JOB_MANAGER_PROCESS_SPEC);

	private JavaCmdJobManagerDecorator javaCmdJobManagerDecorator;

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, FLINK_CONF_DIR_IN_POD);
		flinkConfig.set(KubernetesConfigOptions.FLINK_LOG_DIR, FLINK_LOG_DIR_IN_POD);
		flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, ENTRY_POINT_CLASS);
		flinkConfig.set(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, KUBERNETES_ENTRY_PATH);
	}

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();

		this.javaCmdJobManagerDecorator = new JavaCmdJobManagerDecorator(kubernetesJobManagerParameters);
	}

	@Test
	public void testWhetherContainerOrPodIsReplaced() {
		final FlinkPod resultFlinkPod = javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod);
		assertEquals(baseFlinkPod.getPod(), resultFlinkPod.getPod());
		assertNotEquals(baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
	}

	@Test
	public void testStartCommandWithoutLog4jAndLogback() {
		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = getJobManagerExpectedCommand("", "");
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);

		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLog4j() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = getJobManagerExpectedCommand("", log4j);
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = getJobManagerExpectedCommand("", logback);
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLog4jAndLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand =
				getJobManagerExpectedCommand("", logback + " " + log4j);
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLogAndJVMOpts() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		flinkConfig.set(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand =
				getJobManagerExpectedCommand(jvmOpts, logback + " " + log4j);
		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	@Test
	public void testStartCommandWithLogAndJMOpts() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		flinkConfig.set(CoreOptions.FLINK_JM_JVM_OPTIONS, jvmOpts);
		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());
		final String expectedCommand = getJobManagerExpectedCommand(jvmOpts, logback + " " + log4j);
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

		final String jmJvmOpts = "-DjmJvm";
		this.flinkConfig.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		this.flinkConfig.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = java + " 1 " + classpath + " 2 " + jmJvmMem +
				" " + jvmOpts + " " + jmJvmOpts +
				" " + jmLogfile + " " + logback + " " + log4j +
				" " + ENTRY_POINT_CLASS + " " + jmDynamicProperties;

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

		final String jmJvmOpts = "-DjmJvm";
		this.flinkConfig.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		this.flinkConfig.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertEquals(Collections.singletonList(KUBERNETES_ENTRY_PATH), resultMainContainer.getCommand());

		final String expectedCommand = java + " " + jmJvmMem +
				" " + jmLogfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + jmJvmOpts +
				" " + ENTRY_POINT_CLASS + " " + jmDynamicProperties;

		final List<String> expectedArgs = Arrays.asList(NATIVE_KUBERNETES_COMMAND, expectedCommand);
		assertEquals(expectedArgs, resultMainContainer.getArgs());
	}

	private String getJobManagerExpectedCommand(String jvmAllOpts, String logging) {
		return java + " " + classpath + " " + jmJvmMem +
				(jvmAllOpts.isEmpty() ? "" : " " + jvmAllOpts) +
				(logging.isEmpty() ? "" : " " + jmLogfile + " " + logging) +
				" " + ENTRY_POINT_CLASS + " " + jmDynamicProperties;
	}
}

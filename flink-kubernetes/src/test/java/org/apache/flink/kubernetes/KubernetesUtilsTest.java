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

package org.apache.flink.kubernetes;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link KubernetesUtils}.
 */
public class KubernetesUtilsTest extends TestLogger {

	private static final String java = "$JAVA_HOME/bin/java";
	private static final String classpath = "-classpath $FLINK_CLASSPATH";
	private static final String jvmOpts = "-Djvm";
	private static final String mainClass = "org.apache.flink.kubernetes.KubernetesUtilsTest";
	private static final String mainClassArgs = "--job-id=1 -Dtest.key=value";

	// Logging variables
	private static final String confDirInPod = "/opt/flink/conf";
	private static final String logDirInPod = "/opt/flink/log";
	private static final String logback = String.format("-Dlogback.configurationFile=file:%s/logback.xml", confDirInPod);
	private static final String log4j = String.format("-Dlog4j.configuration=file:%s/log4j.properties", confDirInPod);
	private static final String jmLogfile = String.format("-Dlog.file=%s/jobmanager.log", logDirInPod);
	private static final String jmLogRedirects = String.format("1> %s/jobmanager.out 2> %s/jobmanager.err", logDirInPod, logDirInPod);
	private static final String tmLogfile = String.format("-Dlog.file=%s/taskmanager.log", logDirInPod);
	private static final String tmLogRedirects = String.format("1> %s/taskmanager.out 2> %s/taskmanager.err", logDirInPod, logDirInPod);

	// Memory variables
	private static final int jobManagerMem = 768;
	private static final String jmJvmMem = "-Xms168m -Xmx168m";

	private static final TaskExecutorResourceSpec taskExecutorResourceSpec = new TaskExecutorResourceSpec(
		new CPUResource(1.0),
		new MemorySize(0), // frameworkHeapSize
		new MemorySize(0), // frameworkOffHeapSize
		new MemorySize(111), // taskHeapSize
		new MemorySize(0), // taskOffHeapSize
		new MemorySize(222), // shuffleMemSize
		new MemorySize(0), // managedMemorySize
		new MemorySize(333), // jvmMetaspaceSize
		new MemorySize(0)); // jvmOverheadSize

	private static final String tmJvmMem = "-Xmx111 -Xms111 -XX:MaxDirectMemorySize=222 -XX:MaxMetaspaceSize=333";
	private static final String tmMemDynamicProperties =
		TaskExecutorResourceUtils.generateDynamicConfigsStr(taskExecutorResourceSpec).trim();

	@Test
	public void testGetJobManagerStartCommand() {
		final Configuration cfg = new Configuration();

		final String jmJvmOpts = "-DjmJvm";

		assertEquals(
			getJobManagerExpectedCommand("", "", mainClassArgs),
			getJobManagerStartCommand(cfg, false, false, mainClassArgs));

		// logback only
		assertEquals(
			getJobManagerExpectedCommand("", logback, mainClassArgs),
			getJobManagerStartCommand(cfg, true, false, mainClassArgs));

		// log4j only
		assertEquals(
			getJobManagerExpectedCommand("", log4j, mainClassArgs),
			getJobManagerStartCommand(cfg, false, true, mainClassArgs));

		// logback + log4j
		assertEquals(
			getJobManagerExpectedCommand("", logback + " " + log4j, mainClassArgs),
			getJobManagerStartCommand(cfg, true, true, mainClassArgs));

		// logback + log4j, different JVM opts
		cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		assertEquals(
			getJobManagerExpectedCommand(jvmOpts, logback + " " + log4j, mainClassArgs),
			getJobManagerStartCommand(cfg, true, true, mainClassArgs));

		// logback + log4j,different TM JVM opts
		cfg.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);
		assertEquals(
			getJobManagerExpectedCommand(jvmOpts + " " + jmJvmOpts, logback + " " + log4j, mainClassArgs),
			getJobManagerStartCommand(cfg, true, true, mainClassArgs));

		// no args
		assertEquals(
			getJobManagerExpectedCommand(jvmOpts + " " + jmJvmOpts, logback + " " + log4j, ""),
			getJobManagerStartCommand(cfg, true, true, ""));

		// now try some configurations with different container-start-command-template

		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
			"%java% 1 %classpath% 2 %jvmmem% %jvmopts% %logging% %class% %args% %redirects%");
		assertEquals(
			java + " 1 " + classpath + " 2 " + jmJvmMem +
				" " + jvmOpts + " " + jmJvmOpts + // jvmOpts
				" " + jmLogfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + jmLogRedirects,
			getJobManagerStartCommand(cfg, true, true, mainClassArgs));

		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
			"%java% %jvmmem% %logging% %jvmopts% %class% %args% %redirects%");
		assertEquals(
			java + " " + jmJvmMem +
				" " + jmLogfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + jmJvmOpts + // jvmOpts
				" " + mainClass + " " + mainClassArgs + " " + jmLogRedirects,
			getJobManagerStartCommand(cfg, true, true, mainClassArgs));

	}

	@Test
	public void testGetTaskManagerStartCommand() {
		final Configuration cfg = new Configuration();

		final String tmJvmOpts = "-DtmJvm"; // if set

		assertEquals(
			getTaskManagerExpectedCommand("", "", mainClassArgs),
			getTaskManagerStartCommand(cfg, false, false, mainClassArgs));

		// logback only
		assertEquals(
			getTaskManagerExpectedCommand("", logback, mainClassArgs),
			getTaskManagerStartCommand(cfg, true, false, mainClassArgs));

		// log4j only
		assertEquals(
			getTaskManagerExpectedCommand("", log4j, mainClassArgs),
			getTaskManagerStartCommand(cfg, false, true, mainClassArgs));

		// logback + log4j
		assertEquals(
			getTaskManagerExpectedCommand("", logback + " " + log4j, mainClassArgs),
			getTaskManagerStartCommand(cfg, true, true, mainClassArgs));

		// logback + log4j, different JVM opts
		cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		assertEquals(
			getTaskManagerExpectedCommand(jvmOpts, logback + " " + log4j, mainClassArgs),
			getTaskManagerStartCommand(cfg, true, true, mainClassArgs));

		// logback + log4j, different TM JVM opts
		cfg.setString(CoreOptions.FLINK_TM_JVM_OPTIONS, tmJvmOpts);
		assertEquals(
			getTaskManagerExpectedCommand(jvmOpts + " " + tmJvmOpts, logback + " " + log4j, mainClassArgs),
			getTaskManagerStartCommand(cfg, true, true, mainClassArgs));

		// no args
		assertEquals(
			getTaskManagerExpectedCommand(jvmOpts + " " + tmJvmOpts, logback + " " + log4j, ""),
			getTaskManagerStartCommand(cfg, true, true, null));

		// now try some configurations with different container-start-command-template

		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
			"%java% 1 %classpath% 2 %jvmmem% %jvmopts% %logging% %class% %args% %redirects%");
		assertEquals(
			java + " 1 " + classpath + " 2 " + tmJvmMem +
				" " + jvmOpts + " " + tmJvmOpts + // jvmOpts
				" " + tmLogfile + " " + logback + " " + log4j +
				" " + mainClass + " " + tmMemDynamicProperties + " " + mainClassArgs + " " + tmLogRedirects,
			getTaskManagerStartCommand(cfg, true, true, mainClassArgs));

		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
			"%java% %jvmmem% %logging% %jvmopts% %class% %redirects%");
		assertEquals(
			java + " " + tmJvmMem +
				" " + tmLogfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + tmJvmOpts + // jvmOpts
				" " + mainClass + " " + tmLogRedirects,
			getTaskManagerStartCommand(cfg, true, true, mainClassArgs));

	}

	@Test
	public void testParsePortRange() {
		final Configuration cfg = new Configuration();
		cfg.set(BlobServerOptions.PORT, "50100-50200");
		try {
			KubernetesUtils.parsePort(cfg, BlobServerOptions.PORT);
			fail("Should fail with an exception.");
		} catch (FlinkRuntimeException e) {
			assertThat(
				e.getMessage(),
				containsString(BlobServerOptions.PORT.key() + " should be specified to a fixed port. Do not support a range of ports."));
		}
	}

	@Test
	public void testParsePortNull() {
		final Configuration cfg = new Configuration();
		ConfigOption<String> testingPort = ConfigOptions.key("test.port").stringType().noDefaultValue();
		try {
			KubernetesUtils.parsePort(cfg, testingPort);
			fail("Should fail with an exception.");
		} catch (NullPointerException e) {
			assertThat(
				e.getMessage(),
				containsString(testingPort.key() + " should not be null."));
		}
	}

	private String getJobManagerExpectedCommand(String jvmAllOpts, String logging, String mainClassArgs) {
		return java + " " + classpath + " " + jmJvmMem +
			(jvmAllOpts.isEmpty() ? "" : " " + jvmAllOpts) +
			(logging.isEmpty() ? "" : " " + jmLogfile + " " + logging) +
			" " + mainClass + (mainClassArgs.isEmpty() ? "" : " " + mainClassArgs) + " " + jmLogRedirects;
	}

	private String getTaskManagerExpectedCommand(String jvmAllOpts, String logging, String mainClassArgs) {
		return java + " " + classpath + " " + tmJvmMem +
			(jvmAllOpts.isEmpty() ? "" : " " + jvmAllOpts) +
			(logging.isEmpty() ? "" : " " + tmLogfile + " " + logging) +
			" " + mainClass + " " + tmMemDynamicProperties + (mainClassArgs.isEmpty() ? "" : " " + mainClassArgs) + " " + tmLogRedirects;
	}

	private String getJobManagerStartCommand(
		Configuration cfg,
		boolean hasLogBack,
		boolean hasLog4j,
		String mainClassArgs) {
		return KubernetesUtils.getJobManagerStartCommand(
			cfg,
			jobManagerMem,
			confDirInPod,
			logDirInPod,
			hasLogBack,
			hasLog4j,
			mainClass,
			mainClassArgs
		);
	}

	private String getTaskManagerStartCommand(
			Configuration cfg,
			boolean hasLogBack,
			boolean hasLog4j,
			String mainClassArgs) {

		final ContaineredTaskManagerParameters containeredParams =
			new ContaineredTaskManagerParameters(taskExecutorResourceSpec, 4, new HashMap<>());

		return KubernetesUtils.getTaskManagerStartCommand(
			cfg,
			containeredParams,
			confDirInPod,
			logDirInPod,
			hasLogBack,
			hasLog4j,
			mainClass,
			mainClassArgs
		);
	}
}

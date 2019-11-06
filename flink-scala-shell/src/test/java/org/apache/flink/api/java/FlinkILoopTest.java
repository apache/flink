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

package org.apache.flink.api.java;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.URL;
import java.util.List;

import scala.Option;
import scala.tools.nsc.Settings;
import scala.tools.nsc.settings.MutableSettings;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for {@link FlinkILoop}.
 */
public class FlinkILoopTest extends TestLogger {

	@Test
	public void testConfigurationForwarding() throws Exception {
		String host = "localhost";
		int port = 6123;
		Configuration configuration = new Configuration();
		configuration.setString("foobar", "foobar");

		FlinkILoop flinkILoop = new FlinkILoop(host, port, configuration, Option.empty());

		Settings settings = new Settings();
		((MutableSettings.BooleanSetting) settings.usejavacp()).value_$eq(true);

		flinkILoop.settings_$eq(settings);
		flinkILoop.createInterpreter();

		ScalaShellRemoteEnvironment env = (ScalaShellRemoteEnvironment) flinkILoop.scalaBenv().getJavaEnv();

		TestPlanExecutor testPlanExecutor = new TestPlanExecutor();
		env.setPlanExecutorFactory((host1, port1, configuration1) -> {
			testPlanExecutor.host = host1;
			testPlanExecutor.port = port1;
			testPlanExecutor.configuration = configuration1;
			return testPlanExecutor;
		});

		env.fromElements(1).output(new DiscardingOutputFormat<>());

		env.execute("Test job");

		assertEquals(host, testPlanExecutor.host);
		assertEquals(port, testPlanExecutor.port);
		assertEquals(configuration, testPlanExecutor.configuration);
	}

	@Test
	public void testConfigurationForwardingStreamEnvironment() throws Exception {
		String host = "localhost";
		int port = 6123;
		Configuration configuration = new Configuration();
		configuration.setString("foobar", "foobar");

		FlinkILoop flinkILoop = new FlinkILoop(host, port, configuration, Option.empty());

		Settings settings = new Settings();
		((MutableSettings.BooleanSetting) settings.usejavacp()).value_$eq(true);

		flinkILoop.settings_$eq(settings);
		flinkILoop.createInterpreter();

		ScalaShellRemoteStreamEnvironment env = (ScalaShellRemoteStreamEnvironment) flinkILoop.scalaSenv().getJavaEnv();

		TestPlanExecutor testPlanExecutor = new TestPlanExecutor();
		env.setPlanExecutorFactory((host1, port1, configuration1) -> {
			testPlanExecutor.host = host1;
			testPlanExecutor.port = port1;
			testPlanExecutor.configuration = configuration1;
			return testPlanExecutor;
		});

		env.fromElements(1).print();

		env.execute("Test job");

		assertEquals(host, testPlanExecutor.host);
		assertEquals(port, testPlanExecutor.port);
		assertEquals(configuration, testPlanExecutor.configuration);
	}

	private static class TestPlanExecutor extends PlanExecutor {
		private String host;
		private int port;
		private Configuration configuration;

		@Override
		public JobExecutionResult executePlan(Pipeline plan, List<URL> jarFiles, List<URL> globalClasspaths) {
			return null;
		}
	}

}

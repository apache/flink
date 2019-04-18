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
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import scala.Option;
import scala.tools.nsc.Settings;
import scala.tools.nsc.settings.MutableSettings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link FlinkILoop}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PlanExecutor.class)
@PowerMockIgnore("javax.tools.*")
public class FlinkILoopTest extends TestLogger {

	@Test
	public void testConfigurationForwarding() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString("foobar", "foobar");
		FlinkILoop flinkILoop = new FlinkILoop("localhost", 6123, configuration, Option.<String[]>empty());

		final TestPlanExecutor testPlanExecutor = new TestPlanExecutor();

		PowerMockito.mockStatic(PlanExecutor.class);
		BDDMockito.given(PlanExecutor.createRemoteExecutor(
			Matchers.anyString(),
			Matchers.anyInt(),
			Matchers.any(Configuration.class),
			Matchers.any(java.util.List.class),
			Matchers.any(java.util.List.class)
		)).willAnswer(new Answer<PlanExecutor>() {
			@Override
			public PlanExecutor answer(InvocationOnMock invocation) throws Throwable {
				testPlanExecutor.setHost((String) invocation.getArguments()[0]);
				testPlanExecutor.setPort((Integer) invocation.getArguments()[1]);
				testPlanExecutor.setConfiguration((Configuration) invocation.getArguments()[2]);
				testPlanExecutor.setJars((List<String>) invocation.getArguments()[3]);
				testPlanExecutor.setGlobalClasspaths((List<String>) invocation.getArguments()[4]);

				return testPlanExecutor;
			}
		});

		Settings settings = new Settings();
		((MutableSettings.BooleanSetting) settings.usejavacp()).value_$eq(true);

		flinkILoop.settings_$eq(settings);
		flinkILoop.createInterpreter();

		ExecutionEnvironment env = flinkILoop.scalaBenv().getJavaEnv();

		env.fromElements(1).output(new DiscardingOutputFormat<Integer>());

		env.execute("Test job");

		Configuration forwardedConfiguration = testPlanExecutor.getConfiguration();

		assertEquals(configuration, forwardedConfiguration);
	}

	@Test
	public void testConfigurationForwardingStreamEnvironment() {
		Configuration configuration = new Configuration();
		configuration.setString("foobar", "foobar");

		FlinkILoop flinkILoop = new FlinkILoop("localhost", 6123, configuration, Option.<String[]>empty());

		StreamExecutionEnvironment streamEnv = flinkILoop.scalaSenv().getJavaEnv();

		assertTrue(streamEnv instanceof RemoteStreamEnvironment);

		RemoteStreamEnvironment remoteStreamEnv = (RemoteStreamEnvironment) streamEnv;

		Configuration forwardedConfiguration = remoteStreamEnv.getClientConfiguration();

		assertEquals(configuration, forwardedConfiguration);
	}

	static class TestPlanExecutor extends PlanExecutor {

		private String host;
		private int port;
		private Configuration configuration;
		private List<String> jars;
		private List<String> globalClasspaths;

		@Override
		public void start() throws Exception {

		}

		@Override
		public void stop() throws Exception {

		}

		@Override
		public boolean isRunning() {
			return false;
		}

		@Override
		public JobExecutionResult executePlan(Plan plan) throws Exception {
			return null;
		}

		@Override
		public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
			return null;
		}

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public Configuration getConfiguration() {
			return configuration;
		}

		public void setConfiguration(Configuration configuration) {
			this.configuration = configuration;
		}

		public List<String> getJars() {
			return jars;
		}

		public void setJars(List<String> jars) {
			this.jars = jars;
		}

		public List<String> getGlobalClasspaths() {
			return globalClasspaths;
		}

		public void setGlobalClasspaths(List<String> globalClasspaths) {
			this.globalClasspaths = globalClasspaths;
		}
	}

}

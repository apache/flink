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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.RemoteExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link RemoteStreamEnvironment}.
 */
public class RemoteStreamExecutionEnvironmentTest extends TestLogger {

	/**
	 * Verifies that the port passed to the RemoteStreamEnvironment is used for connecting to the cluster.
	 */
	@Test
	public void testPortForwarding() throws Exception {
		final String host = "fakeHost";
		final int port = 99;
		final Configuration conf = new Configuration();

		final RemoteStreamEnvironment env = (RemoteStreamEnvironment) StreamExecutionEnvironment.createRemoteEnvironment(
			host,
			port,
			conf);

		final JobID jobID = new JobID();

		env.setPlanExecutorFactory((_host, _port, _conf) -> {
			assertThat(_host, is(host));
			assertThat(_port, is(port));
			assertThat(_conf, is(conf));
			return new RemoteExecutor(_host, _port, _conf) {
				@Override
				public JobExecutionResult executePlan(Pipeline plan, List<URL> jarFiles, List<URL> classpathList) {
					return new JobExecutionResult(jobID, 0, Collections.emptyMap());
				}
			};
		});

		env.fromElements(1).map(x -> x * 2);
		JobExecutionResult actualResult = env.execute();
		assertThat(actualResult.getJobID(), is(jobID));
	}

	@Test
	public void testRemoteExecutionWithSavepoint() throws Exception {
		final String host = "fakeHost";
		final int port = 99;
		final Configuration conf = new Configuration();
		final SavepointRestoreSettings settings = SavepointRestoreSettings.forPath("fakePath");

		final RemoteStreamEnvironment env = new RemoteStreamEnvironment(host, port, conf, new String[0], new URL[0], settings);

		final JobID jobID = new JobID();

		env.setPlanExecutorFactory((_host, _port, _conf) -> {
			assertThat(_host, is(host));
			assertThat(_port, is(port));
			assertThat(_conf, is(conf));
			return new RemoteExecutor(_host, _port, _conf) {
				@Override
				public JobExecutionResult executePlan(Pipeline plan, List<URL> jarFiles, List<URL> classpathList) {
					return new JobExecutionResult(jobID, 0, Collections.emptyMap());
				}
			};
		});

		env.fromElements(1).map(x -> x * 2);
		JobExecutionResult actualResult = env.execute();
		assertThat(actualResult.getJobID(), is(jobID));
	}
}

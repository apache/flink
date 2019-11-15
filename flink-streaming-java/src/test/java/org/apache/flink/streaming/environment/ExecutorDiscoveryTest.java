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

package org.apache.flink.streaming.environment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.core.execution.ExecutorFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.OptionalFailure;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Tests the {@link ExecutorFactory} discovery in the {@link StreamExecutionEnvironment}.
 */
public class ExecutorDiscoveryTest {

	@Test
	public void correctExecutorShouldBeInstantiatedBasedOnConfigurationOption() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.set(DeploymentOptions.TARGET, IDReportingExecutorFactory.ID);

		final JobExecutionResult result = executeTestJobBasedOnConfig(configuration);

		final String executorName = result.getAllAccumulatorResults().get(DeploymentOptions.TARGET.key()).toString();
		assertThat(executorName, is(equalTo(IDReportingExecutorFactory.ID)));
	}

	private JobExecutionResult executeTestJobBasedOnConfig(final Configuration configuration) throws Exception {
		final StreamExecutionEnvironment env = new StreamExecutionEnvironment(configuration);
		env.fromCollection(Collections.singletonList(42))
				.addSink(new DiscardingSink<>());
		return env.execute();
	}

	/**
	 * An {@link ExecutorFactory} that returns an {@link Executor} that instead of executing, it simply
	 * returns its name in the {@link JobExecutionResult}.
	 */
	public static class IDReportingExecutorFactory implements ExecutorFactory {

		public static final String ID = "test-executor-A";

		@Override
		public boolean isCompatibleWith(Configuration configuration) {
			return ID.equals(configuration.get(DeploymentOptions.TARGET));
		}

		@Override
		public Executor getExecutor(Configuration configuration) {
			return (pipeline, executionConfig) -> {
				final Map<String, OptionalFailure<Object>> res = new HashMap<>();
				res.put(DeploymentOptions.TARGET.key(), OptionalFailure.of(ID));
				return new JobExecutionResult(new JobID(), 12L, res);
			};
		}
	}
}

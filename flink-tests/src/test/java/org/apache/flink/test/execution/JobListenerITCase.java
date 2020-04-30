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

package org.apache.flink.test.execution;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link JobListener}.
 */
public class JobListenerITCase extends TestLogger {

	@ClassRule
	public static MiniClusterWithClientResource miniClusterResource = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
					.build());

	private static Configuration getClientConfiguration() {
		Configuration result = new Configuration(miniClusterResource.getClientConfiguration());
		result.set(DeploymentOptions.TARGET, RemoteExecutor.NAME);
		return result;
	}

	@Test
	public void testExecuteCallsJobListenerOnBatchEnvironment() throws Exception {
		AtomicReference<JobID> jobIdReference = new AtomicReference<>();
		OneShotLatch submissionLatch = new OneShotLatch();
		OneShotLatch executionLatch = new OneShotLatch();

		ExecutionEnvironment env = new ExecutionEnvironment(getClientConfiguration());

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient, Throwable t) {
				jobIdReference.set(jobClient.getJobID());
				submissionLatch.trigger();
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
				executionLatch.trigger();
			}
		});

		env.fromElements(1, 2, 3, 4, 5).output(new DiscardingOutputFormat<>());
		JobExecutionResult jobExecutionResult = env.execute();

		submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
		executionLatch.await(2000L, TimeUnit.MILLISECONDS);

		assertThat(jobExecutionResult.getJobID(), is(jobIdReference.get()));
	}

	@Test
	public void testExecuteAsyncCallsJobListenerOnBatchEnvironment() throws Exception {
		AtomicReference<JobID> jobIdReference = new AtomicReference<>();
		OneShotLatch submissionLatch = new OneShotLatch();

		ExecutionEnvironment env = new ExecutionEnvironment(getClientConfiguration());

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient, Throwable t) {
				jobIdReference.set(jobClient.getJobID());
				submissionLatch.trigger();
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
			}
		});

		env.fromElements(1, 2, 3, 4, 5).output(new DiscardingOutputFormat<>());
		JobClient jobClient = env.executeAsync();

		submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
		// when executing asynchronously we don't get an "executed" callback

		assertThat(jobClient.getJobID(), is(jobIdReference.get()));
	}

	@Test
	public void testExecuteCallsJobListenerOnMainThreadOnBatchEnvironment() throws Exception {
		AtomicReference<Thread> threadReference = new AtomicReference<>();

		ExecutionEnvironment env = new ExecutionEnvironment(getClientConfiguration());

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient, Throwable t) {
				threadReference.set(Thread.currentThread());
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
			}
		});

		env.fromElements(1, 2, 3, 4, 5).output(new DiscardingOutputFormat<>());
		env.execute();

		assertThat(Thread.currentThread(), is(threadReference.get()));
	}

	@Test
	public void testExecuteAsyncCallsJobListenerOnMainThreadOnBatchEnvironment() throws Exception {
		AtomicReference<Thread> threadReference = new AtomicReference<>();

		ExecutionEnvironment env = new ExecutionEnvironment(getClientConfiguration());

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient, Throwable t) {
				threadReference.set(Thread.currentThread());
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
			}
		});

		env.fromElements(1, 2, 3, 4, 5).output(new DiscardingOutputFormat<>());
		env.executeAsync();

		assertThat(Thread.currentThread(), is(threadReference.get()));
	}

	@Test
	public void testExecuteCallsJobListenerOnStreamingEnvironment() throws Exception {
		AtomicReference<JobID> jobIdReference = new AtomicReference<>();
		OneShotLatch submissionLatch = new OneShotLatch();
		OneShotLatch executionLatch = new OneShotLatch();

		StreamExecutionEnvironment env = new StreamExecutionEnvironment(getClientConfiguration());

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient, Throwable t) {
				jobIdReference.set(jobClient.getJobID());
				submissionLatch.trigger();
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
				executionLatch.trigger();
			}
		});

		env.fromElements(1, 2, 3, 4, 5).addSink(new DiscardingSink<>());
		JobExecutionResult jobExecutionResult = env.execute();

		submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
		executionLatch.await(2000L, TimeUnit.MILLISECONDS);

		assertThat(jobExecutionResult.getJobID(), is(jobIdReference.get()));
	}

	@Test
	public void testExecuteAsyncCallsJobListenerOnStreamingEnvironment() throws Exception {
		AtomicReference<JobID> jobIdReference = new AtomicReference<>();
		OneShotLatch submissionLatch = new OneShotLatch();

		StreamExecutionEnvironment env = new StreamExecutionEnvironment(getClientConfiguration());

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient, Throwable t) {
				jobIdReference.set(jobClient.getJobID());
				submissionLatch.trigger();
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
			}
		});

		env.fromElements(1, 2, 3, 4, 5).addSink(new DiscardingSink<>());
		JobClient jobClient = env.executeAsync();

		submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
		// when executing asynchronously we don't get an "executed" callback

		assertThat(jobClient.getJobID(), is(jobIdReference.get()));
	}

	@Test
	public void testExecuteCallsJobListenerOnMainThreadOnStreamEnvironment() throws Exception {
		AtomicReference<Thread> threadReference = new AtomicReference<>();

		StreamExecutionEnvironment env = new StreamExecutionEnvironment(getClientConfiguration());

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient, Throwable t) {
				threadReference.set(Thread.currentThread());
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
			}
		});

		env.fromElements(1, 2, 3, 4, 5).addSink(new DiscardingSink<>());
		env.execute();

		assertThat(Thread.currentThread(), is(threadReference.get()));
	}

	@Test
	public void testExecuteAsyncCallsJobListenerOnMainThreadOnStreamEnvironment() throws Exception {
		AtomicReference<Thread> threadReference = new AtomicReference<>();

		StreamExecutionEnvironment env = new StreamExecutionEnvironment(getClientConfiguration());

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient, Throwable t) {
				threadReference.set(Thread.currentThread());
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
			}
		});

		env.fromElements(1, 2, 3, 4, 5).addSink(new DiscardingSink<>());
		env.executeAsync();

		assertThat(Thread.currentThread(), is(threadReference.get()));
	}
}

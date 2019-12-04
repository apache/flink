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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link JobListener}.
 */
public class JobListenerTest extends TestLogger {

	@Test
	public void testJobListenerOnBatchEnvironment() throws Exception {
		OneShotLatch submissionLatch = new OneShotLatch();
		OneShotLatch executionLatch = new OneShotLatch();

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient) {
				submissionLatch.trigger();
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
				executionLatch.trigger();
			}
		});

		env.fromElements(1, 2, 3, 4, 5).output(new DiscardingOutputFormat<>());
		env.execute();

		submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
		executionLatch.await(2000L, TimeUnit.MILLISECONDS);
	}

	@Test
	public void testJobListenerOnStreamingEnvironment() throws Exception {
		OneShotLatch submissionLatch = new OneShotLatch();
		OneShotLatch executionLatch = new OneShotLatch();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.registerJobListener(new JobListener() {
			@Override
			public void onJobSubmitted(JobClient jobClient) {
				submissionLatch.trigger();
			}

			@Override
			public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
				executionLatch.trigger();
			}
		});

		env.fromElements(1, 2, 3, 4, 5).addSink(new DiscardingSink<>());
		env.execute();

		submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
		executionLatch.await(2000L, TimeUnit.MILLISECONDS);
	}

}

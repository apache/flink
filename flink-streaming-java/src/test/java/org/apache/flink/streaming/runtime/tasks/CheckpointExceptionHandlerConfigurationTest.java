/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test that the configuration mechanism for how tasks react on checkpoint errors works correctly.
 */
public class CheckpointExceptionHandlerConfigurationTest extends TestLogger {

	@Test
	public void testConfigurationFailOnException() throws Exception {
		testConfigForwarding(true);
	}

	@Test
	public void testConfigurationDeclineOnException() throws Exception {
		testConfigForwarding(false);
	}

	@Test
	public void testFailIsDefaultConfig() {
		ExecutionConfig newExecutionConfig = new ExecutionConfig();
		Assert.assertTrue(newExecutionConfig.isFailTaskOnCheckpointError());
	}

	private void testConfigForwarding(boolean failOnException) throws Exception {

		final boolean expectedHandlerFlag = failOnException;

		final DummyEnvironment environment = new DummyEnvironment("test", 1, 0);
		environment.setTaskStateManager(new TestTaskStateManager());
		environment.getExecutionConfig().setFailTaskOnCheckpointError(expectedHandlerFlag);

		final CheckpointExceptionHandlerFactory inspectingFactory = new CheckpointExceptionHandlerFactory() {

			@Override
			public CheckpointExceptionHandler createCheckpointExceptionHandler(
				boolean failTaskOnCheckpointException,
				Environment environment) {

				Assert.assertEquals(expectedHandlerFlag, failTaskOnCheckpointException);
				return super.createCheckpointExceptionHandler(failTaskOnCheckpointException, environment);
			}
		};

		StreamTask streamTask = new StreamTask(environment, null) {
			@Override
			protected void init() throws Exception {}

			@Override
			protected void run() throws Exception {}

			@Override
			protected void cleanup() throws Exception {}

			@Override
			protected void cancelTask() throws Exception {}

			@Override
			protected CheckpointExceptionHandlerFactory createCheckpointExceptionHandlerFactory() {
				return inspectingFactory;
			}
		};

		streamTask.invoke();
	}

	@Test
	public void testCheckpointConfigDefault() throws Exception {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		Assert.assertTrue(streamExecutionEnvironment.getCheckpointConfig().isFailOnCheckpointingErrors());
	}

	@Test
	public void testPropagationFailFromCheckpointConfig() throws Exception {
		doTestPropagationFromCheckpointConfig(true);
	}

	@Test
	public void testPropagationDeclineFromCheckpointConfig() throws Exception {
		doTestPropagationFromCheckpointConfig(false);
	}

	public void doTestPropagationFromCheckpointConfig(boolean failTaskOnCheckpointErrors) throws Exception {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		streamExecutionEnvironment.setParallelism(1);
		streamExecutionEnvironment.getCheckpointConfig().setCheckpointInterval(1000);
		streamExecutionEnvironment.getCheckpointConfig().setFailOnCheckpointingErrors(failTaskOnCheckpointErrors);
		streamExecutionEnvironment.addSource(new SourceFunction<Integer>() {

			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}

		}).addSink(new DiscardingSink<>());

		StreamGraph streamGraph = streamExecutionEnvironment.getStreamGraph();
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		SerializedValue<ExecutionConfig> serializedExecutionConfig = jobGraph.getSerializedExecutionConfig();
		ExecutionConfig executionConfig =
			serializedExecutionConfig.deserializeValue(Thread.currentThread().getContextClassLoader());

		Assert.assertEquals(failTaskOnCheckpointErrors, executionConfig.isFailTaskOnCheckpointError());
	}
}

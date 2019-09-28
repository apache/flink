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

import org.apache.flink.runtime.checkpoint.CheckpointFailureManager;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test that the configuration mechanism for how tasks react on checkpoint errors works correctly.
 */
public class CheckpointExceptionHandlerConfigurationTest extends TestLogger {

	@Test
	public void testCheckpointConfigDefault() {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		CheckpointConfig checkpointConfig = streamExecutionEnvironment.getCheckpointConfig();
		Assert.assertTrue(checkpointConfig.isFailOnCheckpointingErrors());
		Assert.assertEquals(0, checkpointConfig.getTolerableCheckpointFailureNumber());
	}

	@Test
	public void testSetCheckpointConfig() {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		CheckpointConfig checkpointConfig = streamExecutionEnvironment.getCheckpointConfig();

		// use deprecated API to set not fail on checkpoint errors
		checkpointConfig.setFailOnCheckpointingErrors(false);
		Assert.assertFalse(checkpointConfig.isFailOnCheckpointingErrors());
		Assert.assertEquals(CheckpointFailureManager.UNLIMITED_TOLERABLE_FAILURE_NUMBER, checkpointConfig.getTolerableCheckpointFailureNumber());

		// use new API to set tolerable declined checkpoint number
		checkpointConfig.setTolerableCheckpointFailureNumber(5);
		Assert.assertEquals(5, checkpointConfig.getTolerableCheckpointFailureNumber());

		// after we configure the tolerable declined checkpoint number, deprecated API would not take effect
		checkpointConfig.setFailOnCheckpointingErrors(true);
		Assert.assertEquals(5, checkpointConfig.getTolerableCheckpointFailureNumber());
	}

	@Test
	public void testPropagationFailFromCheckpointConfig() {
		try {
			doTestPropagationFromCheckpointConfig(true);
		} catch (IllegalArgumentException ignored) {
			// ignored
		}
	}

	@Test
	public void testPropagationDeclineFromCheckpointConfig() {
		doTestPropagationFromCheckpointConfig(false);
	}

	public void doTestPropagationFromCheckpointConfig(boolean failTaskOnCheckpointErrors) {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		streamExecutionEnvironment.setParallelism(1);
		streamExecutionEnvironment.getCheckpointConfig().setCheckpointInterval(1000);
		streamExecutionEnvironment.getCheckpointConfig().setFailOnCheckpointingErrors(failTaskOnCheckpointErrors);
		streamExecutionEnvironment.addSource(new SourceFunction<Integer>() {

			@Override
			public void run(SourceContext<Integer> ctx) {
			}

			@Override
			public void cancel() {
			}

		}).addSink(new DiscardingSink<>());
	}
}

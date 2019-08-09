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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.co.CoStreamMap;
import org.apache.flink.streaming.runtime.io.CheckpointBarrierAlignerTestBase;
import org.apache.flink.streaming.runtime.io.CheckpointBarrierAlignerTestBase.CheckpointExceptionMatcher;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTest.NoOpStreamTask;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Test checkpoint cancellation barrier.
 */
public class StreamTaskCancellationBarrierTest {

	/**
	 * This test checks that tasks emit a proper cancel checkpoint barrier, if a "trigger checkpoint" message
	 * comes before they are ready.
	 */
	@Test
	public void testEmitCancellationBarrierWhenNotReady() throws Exception {
		StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
				InitBlockingTask::new, BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		// start the test - this cannot succeed across the 'init()' method
		testHarness.invoke();

		StreamTask<String, ?> task = testHarness.getTask();

		// tell the task to commence a checkpoint
		boolean result = task.triggerCheckpoint(new CheckpointMetaData(41L, System.currentTimeMillis()),
			CheckpointOptions.forCheckpointWithDefaultLocation(), false);
		assertFalse("task triggered checkpoint though not ready", result);

		// a cancellation barrier should be downstream
		Object emitted = testHarness.getOutput().poll();
		assertNotNull("nothing emitted", emitted);
		assertTrue("wrong type emitted", emitted instanceof CancelCheckpointMarker);
		assertEquals("wrong checkpoint id", 41L, ((CancelCheckpointMarker) emitted).getCheckpointId());
	}

	/**
	 * This test verifies (for onw input tasks) that the Stream tasks react the following way to
	 * receiving a checkpoint cancellation barrier:
	 *   - send a "decline checkpoint" notification out (to the JobManager)
	 *   - emit a cancellation barrier downstream.
	 */
	@Test
	public void testDeclineCallOnCancelBarrierOneInput() throws Exception {

		OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
				OneInputStreamTask::new,
				1, 2,
				BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamMap<String, String> mapOperator = new StreamMap<>(new IdentityMap());
		streamConfig.setStreamOperator(mapOperator);
		streamConfig.setOperatorID(new OperatorID());

		StreamMockEnvironment environment = spy(testHarness.createEnvironment());

		// start the task
		testHarness.invoke(environment);
		testHarness.waitForTaskRunning();

		// emit cancellation barriers
		testHarness.processEvent(new CancelCheckpointMarker(2L), 0, 1);
		testHarness.processEvent(new CancelCheckpointMarker(2L), 0, 0);
		testHarness.waitForInputProcessing();

		// the decline call should go to the coordinator
		verify(environment, times(1)).declineCheckpoint(eq(2L),
			argThat(new CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));

		// a cancellation barrier should be downstream
		Object result = testHarness.getOutput().poll();
		assertNotNull("nothing emitted", result);
		assertTrue("wrong type emitted", result instanceof CancelCheckpointMarker);
		assertEquals("wrong checkpoint id", 2L, ((CancelCheckpointMarker) result).getCheckpointId());

		// cancel and shutdown
		testHarness.endInput();
		testHarness.waitForTaskCompletion();
	}

	/**
	 * This test verifies (for two input tasks) that the Stream tasks react the following way to
	 * receiving a checkpoint cancellation barrier:
	 *   - send a "decline checkpoint" notification out (to the JobManager)
	 *   - emit a cancellation barrier downstream.
	 */
	@Test
	public void testDeclineCallOnCancelBarrierTwoInputs() throws Exception {

		TwoInputStreamTaskTestHarness<String, String, String> testHarness = new TwoInputStreamTaskTestHarness<>(
				TwoInputStreamTask::new,
				BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		CoStreamMap<String, String, String> op = new CoStreamMap<>(new UnionCoMap());
		streamConfig.setStreamOperator(op);
		streamConfig.setOperatorID(new OperatorID());

		StreamMockEnvironment environment = spy(testHarness.createEnvironment());

		// start the task
		testHarness.invoke(environment);
		testHarness.waitForTaskRunning();

		// emit cancellation barriers
		testHarness.processEvent(new CancelCheckpointMarker(2L), 0, 0);
		testHarness.processEvent(new CancelCheckpointMarker(2L), 1, 0);
		testHarness.waitForInputProcessing();

		// the decline call should go to the coordinator
		verify(environment, times(1)).declineCheckpoint(eq(2L),
			argThat(new CheckpointBarrierAlignerTestBase.CheckpointExceptionMatcher(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));

		// a cancellation barrier should be downstream
		Object result = testHarness.getOutput().poll();
		assertNotNull("nothing emitted", result);
		assertTrue("wrong type emitted", result instanceof CancelCheckpointMarker);
		assertEquals("wrong checkpoint id", 2L, ((CancelCheckpointMarker) result).getCheckpointId());

		// cancel and shutdown
		testHarness.endInput();
		testHarness.waitForTaskCompletion();
	}

	// ------------------------------------------------------------------------
	//  test tasks / functions
	// ------------------------------------------------------------------------

	private static class InitBlockingTask extends NoOpStreamTask<String, AbstractStreamOperator<String>> {

		private final Object lock = new Object();
		private volatile boolean running = true;

		protected InitBlockingTask(Environment env) {
			super(env);
		}

		@Override
		protected void init() throws Exception {
			super.init();
			synchronized (lock) {
				while (running) {
					lock.wait();
				}
			}
		}

		@Override
		protected void cancelTask() throws Exception {
			running = false;
			synchronized (lock) {
				lock.notifyAll();
			}
		}
	}

	private static class IdentityMap implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map(String value) throws Exception {
			return value;
		}
	}

	private static class UnionCoMap implements CoMapFunction<String, String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map1(String value) throws Exception {
			return value;
		}

		@Override
		public String map2(String value) throws Exception {
			return value;
		}
	}
}

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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ExecutionFailureHandler}.
 */
public class ExecutionFailureHandlerTest extends TestLogger {

	/**
	 * Tests the case that task restarting is accepted.
	 */
	@Test
	public void testNormalFailureHandling() throws Exception {
		ExecutionFailureHandler executionFailureHandler = createExecutionFailureHandler();
		FailoverStrategy failoverStrategy = executionFailureHandler.getFailoverStrategy();
		RestartBackoffTimeStrategy restartStrategy = executionFailureHandler.getRestartBackoffTimeStrategy();

		Set<ExecutionVertexID> tasksToBeRestarted = new HashSet<>();
		when(failoverStrategy.getTasksNeedingRestart(any(ExecutionVertexID.class), any(Throwable.class))).thenReturn(
			tasksToBeRestarted);

		long restartDelayMs = 1234;
		when(restartStrategy.getBackoffTime()).thenReturn(restartDelayMs);

		// accept restarts
		when(restartStrategy.canRestart()).thenReturn(true);

		FailureHandlingResult result = executionFailureHandler.getFailureHandlingResult(
			new ExecutionVertexID(new JobVertexID(), 0),
			new Exception("test failure"));

		// verify results
		assertTrue(result.canRestart());
		assertEquals(restartDelayMs, result.getRestartDelayMS());
		assertEquals(tasksToBeRestarted, result.getVerticesToBeRestarted());
		assertNull(result.getError());
	}

	/**
	 * Tests the case that task restarting is suppressed.
	 */
	@Test
	public void testRestartingSuppressedFailureHandlingResult() throws Exception {
		ExecutionFailureHandler executionFailureHandler = createExecutionFailureHandler();
		FailoverStrategy failoverStrategy = executionFailureHandler.getFailoverStrategy();
		RestartBackoffTimeStrategy restartStrategy = executionFailureHandler.getRestartBackoffTimeStrategy();

		Set<ExecutionVertexID> tasksToBeRestarted = new HashSet<>();
		when(failoverStrategy.getTasksNeedingRestart(any(ExecutionVertexID.class), any(Throwable.class))).thenReturn(
			tasksToBeRestarted);

		long restartDelayMs = 1234;
		when(restartStrategy.getBackoffTime()).thenReturn(restartDelayMs);

		// suppress restarts
		when(restartStrategy.canRestart()).thenReturn(false);

		FailureHandlingResult result = executionFailureHandler.getFailureHandlingResult(
			new ExecutionVertexID(new JobVertexID(), 0),
			new Exception("test failure"));

		// verify results
		assertFalse(result.canRestart());
		assertTrue(result.getRestartDelayMS() < 0);
		assertNull(result.getVerticesToBeRestarted());
		assertNotNull(result.getError());
	}

	/**
	 * Tests the case that the failure is non-recoverable type.
	 */
	@Test
	public void testNonRecoverableFailureHandlingResult() throws Exception {
		ExecutionFailureHandler executionFailureHandler = createExecutionFailureHandler();
		FailoverStrategy failoverStrategy = executionFailureHandler.getFailoverStrategy();
		RestartBackoffTimeStrategy restartStrategy = executionFailureHandler.getRestartBackoffTimeStrategy();

		Set<ExecutionVertexID> tasksToBeRestarted = new HashSet<>();
		when(failoverStrategy.getTasksNeedingRestart(any(ExecutionVertexID.class), any(Throwable.class))).thenReturn(
			tasksToBeRestarted);

		long restartDelayMs = 1234;
		when(restartStrategy.getBackoffTime()).thenReturn(restartDelayMs);

		// accept restarts
		when(restartStrategy.canRestart()).thenReturn(true);

		FailureHandlingResult result = executionFailureHandler.getFailureHandlingResult(
			new ExecutionVertexID(new JobVertexID(), 0),
			new SuppressRestartsException(new Exception("test failure")));

		// verify results
		assertFalse(result.canRestart());
		assertTrue(result.getRestartDelayMS() < 0);
		assertNull(result.getVerticesToBeRestarted());
		assertNotNull(result.getError());
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private ExecutionFailureHandler createExecutionFailureHandler() {
		FailoverTopology topology = new TestFailoverTopology.Builder().build();

		FailoverStrategy.Factory failoverStrategyFactory = mock(FailoverStrategy.Factory.class);
		when(failoverStrategyFactory.create(any(FailoverTopology.class))).thenReturn(mock(FailoverStrategy.class));

		RestartBackoffTimeStrategy.Factory restartStrategyFactory = mock(RestartBackoffTimeStrategy.Factory.class);
		when(restartStrategyFactory.create()).thenReturn(mock(RestartBackoffTimeStrategy.class));

		return new ExecutionFailureHandler(topology, failoverStrategyFactory, restartStrategyFactory);
	}
}

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

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link ExecutionFailureHandler}.
 */
public class ExecutionFailureHandlerTest extends TestLogger {

	private static final long restartDelayMs = 1234L;

	private TestFailoverStrategy failoverStrategy;

	private TestRestartBackoffTimeStrategy restartStrategy;

	private ExecutionFailureHandler executionFailureHandler;

	@Before
	public void setUp() {
		failoverStrategy = new TestFailoverStrategy();
		restartStrategy = new TestRestartBackoffTimeStrategy(true, restartDelayMs);
		executionFailureHandler = new ExecutionFailureHandler(failoverStrategy, restartStrategy);
	}

	/**
	 * Tests the case that task restarting is accepted.
	 */
	@Test
	public void testNormalFailureHandling() {
		final Set<ExecutionVertexID> tasksToRestart = Collections.singleton(
			new ExecutionVertexID(new JobVertexID(), 0));
		failoverStrategy.setTasksToRestart(tasksToRestart);

		// trigger a task failure
		final FailureHandlingResult result = executionFailureHandler.getFailureHandlingResult(
			new ExecutionVertexID(new JobVertexID(), 0),
			new Exception("test failure"));

		// verify results
		assertTrue(result.canRestart());
		assertEquals(restartDelayMs, result.getRestartDelayMS());
		assertEquals(tasksToRestart, result.getVerticesToRestart());
		try {
			result.getError();
			fail("Cannot get error when the restarting is accepted");
		} catch (IllegalStateException ex) {
			// expected
		}
	}

	/**
	 * Tests the case that task restarting is suppressed.
	 */
	@Test
	public void testRestartingSuppressedFailureHandlingResult() {
		// restart strategy suppresses restarting
		restartStrategy.setCanRestart(false);

		// trigger a task failure
		final FailureHandlingResult result = executionFailureHandler.getFailureHandlingResult(
			new ExecutionVertexID(new JobVertexID(), 0),
			new Exception("test failure"));

		// verify results
		assertFalse(result.canRestart());
		assertNotNull(result.getError());
		assertFalse(ExecutionFailureHandler.isUnrecoverableError(result.getError()));
		try {
			result.getVerticesToRestart();
			fail("get tasks to restart is not allowed when restarting is suppressed");
		} catch (IllegalStateException ex) {
			// expected
		}
		try {
			result.getRestartDelayMS();
			fail("get restart delay is not allowed when restarting is suppressed");
		} catch (IllegalStateException ex) {
			// expected
		}
	}

	/**
	 * Tests the case that the failure is non-recoverable type.
	 */
	@Test
	public void testNonRecoverableFailureHandlingResult() {
		// trigger an unrecoverable task failure
		final FailureHandlingResult result = executionFailureHandler.getFailureHandlingResult(
			new ExecutionVertexID(new JobVertexID(), 0),
			new Exception(new SuppressRestartsException(new Exception("test failure"))));

		// verify results
		assertFalse(result.canRestart());
		assertNotNull(result.getError());
		assertTrue(ExecutionFailureHandler.isUnrecoverableError(result.getError()));
		try {
			result.getVerticesToRestart();
			fail("get tasks to restart is not allowed when restarting is suppressed");
		} catch (IllegalStateException ex) {
			// expected
		}
		try {
			result.getRestartDelayMS();
			fail("get restart delay is not allowed when restarting is suppressed");
		} catch (IllegalStateException ex) {
			// expected
		}
	}

	/**
	 * Tests the check for unrecoverable error.
	 */
	@Test
	public void testUnrecoverableErrorCheck() {
		// normal error
		assertFalse(ExecutionFailureHandler.isUnrecoverableError(new Exception()));

		// direct unrecoverable error
		assertTrue(ExecutionFailureHandler.isUnrecoverableError(new SuppressRestartsException(new Exception())));

		// nested unrecoverable error
		assertTrue(ExecutionFailureHandler.isUnrecoverableError(
			new Exception(new SuppressRestartsException(new Exception()))));
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	/**
	 * A FailoverStrategy implementation for tests. It always suggests restarting the given tasks to restart.
	 */
	private class TestFailoverStrategy implements FailoverStrategy {

		private Set<ExecutionVertexID> tasksToRestart;

		public TestFailoverStrategy() {
		}

		public void setTasksToRestart(final Set<ExecutionVertexID> tasksToRestart) {
			this.tasksToRestart = tasksToRestart;
		}

		@Override
		public Set<ExecutionVertexID> getTasksNeedingRestart(
				final ExecutionVertexID executionVertexId,
				final Throwable cause) {

			return tasksToRestart;
		}
	}

}

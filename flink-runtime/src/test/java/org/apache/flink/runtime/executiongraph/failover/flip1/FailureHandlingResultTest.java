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

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link FailureHandlingResult}.
 */
public class FailureHandlingResultTest extends TestLogger {

	/**
	 * Tests normal FailureHandlingResult.
	 */
	@Test
	public void testNormalFailureHandlingResult() throws Exception {
		// create a normal FailureHandlingResult
		Set<ExecutionVertexID> tasks = new HashSet<>();
		long delay = 1234;
		FailureHandlingResult result = new FailureHandlingResult(tasks, delay);

		assertTrue(result.canRestart());
		assertEquals(delay, result.getRestartDelayMS());
		assertEquals(tasks, result.getVerticesToRestart());
		assertNull(result.getError());
	}

	/**
	 * Tests FailureHandlingResult which suppresses restarts.
	 */
	@Test
	public void testRestartingSuppressedFailureHandlingResult() throws Exception {
		// create a FailureHandlingResult with error
		Throwable error = new Exception("test error");
		FailureHandlingResult result = new FailureHandlingResult(error);

		assertFalse(result.canRestart());
		assertEquals(error, result.getError());
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
}

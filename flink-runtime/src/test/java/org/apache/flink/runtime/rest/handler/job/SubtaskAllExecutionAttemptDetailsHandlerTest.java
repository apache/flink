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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.job.SubtaskAllExecutionAttemptDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAllAttemptsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.EvictingBoundedList;

import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.ExceptionUtils.STRINGIFIED_NULL_EXCEPTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The test of {@link SubtaskAllExecutionAttemptDetailsHandler}.
 */
public class SubtaskAllExecutionAttemptDetailsHandlerTest {
	@Test
	public void testHandleRequest() throws RestHandlerException {

		final int subtaskIndex = 2;

		// Prepare current execution.
		final ExecutionAttemptID currentExecutionAttemptID = new ExecutionAttemptID();
		final int currentExecutionAttempt = 1;
		final ExecutionState currentExecutionExpectedState = ExecutionState.RUNNING;
		final TaskManagerLocation currentExecutionLocation =
			new TaskManagerLocation(ResourceID.generate(),  InetAddress.getLoopbackAddress(), 9527);
		final long[] currentExecutionTimestamps = new long[ExecutionState.values().length];
		final long currentExecutionDeployTimestamps = System.currentTimeMillis() - 1024;
		currentExecutionTimestamps[ExecutionState.DEPLOYING.ordinal()] = currentExecutionDeployTimestamps;

		final ArchivedExecution currentExecution = new ArchivedExecution(
			null,
			null,
			currentExecutionAttemptID,
			currentExecutionAttempt,
			currentExecutionExpectedState,
			STRINGIFIED_NULL_EXCEPTION,
			currentExecutionLocation,
			subtaskIndex,
			currentExecutionTimestamps);

		// Prepare history execution.
		final EvictingBoundedList<ArchivedExecution> priorExecutions = new EvictingBoundedList<>(10);
		final ExecutionAttemptID failedExecutionAttemptID = new ExecutionAttemptID();
		final int failedExecutionAttempt = 0;
		final ExecutionState failedExecutionState = ExecutionState.FAILED;
		final String failureCause = "NullPointerException";
		final TaskManagerLocation failedExecutionLocation =
			new TaskManagerLocation(ResourceID.generate(),  InetAddress.getLoopbackAddress(), 10086);
		final long[] failedExecutionTimestamps = new long[ExecutionState.values().length];
		final long failedExecutionDeployTimestamps = System.currentTimeMillis() - 99999;
		failedExecutionTimestamps[ExecutionState.DEPLOYING.ordinal()] = failedExecutionDeployTimestamps;
		final long failedExecutionFailedTimestamps = System.currentTimeMillis() - 10086;
		failedExecutionTimestamps[ExecutionState.FAILED.ordinal()] = failedExecutionFailedTimestamps;

		final ArchivedExecution failedExecution = new ArchivedExecution(
			null,
			null,
			failedExecutionAttemptID,
			failedExecutionAttempt,
			failedExecutionState,
			failureCause,
			failedExecutionLocation,
			2,
			failedExecutionTimestamps);

		priorExecutions.add(failedExecution);

		final String subtaskName = "subtask-2";
		final ArchivedExecutionVertex archivedExecutionVertex = new ArchivedExecutionVertex(
			subtaskIndex,
			subtaskName,
			currentExecution,
			priorExecutions
		);

		// Invoke testing method.
		final SubtaskAllExecutionAttemptDetailsHandler handler = new SubtaskAllExecutionAttemptDetailsHandler(
			CompletableFuture.completedFuture("127.0.0.1:9527"),
			() -> null,
			Time.milliseconds(100),
			Collections.emptyMap(),
			SubtaskAllExecutionAttemptDetailsHeaders.getInstance(),
			new ExecutionGraphCache(Time.milliseconds(100), Time.milliseconds(100)),
			TestingUtils.defaultExecutor());

		final SubtaskExecutionAllAttemptsInfo attemptsInfo = handler.handleRequest(null, archivedExecutionVertex);

		// Verify
		assertEquals(2, attemptsInfo.getAttempts().size());

		final SubtaskExecutionAttemptInfo[] attemptsInfoArray =
			attemptsInfo.getAttempts().toArray(new SubtaskExecutionAttemptInfo[0]);

		final SubtaskExecutionAttemptInfo runningAttempt = attemptsInfoArray[0];
		assertEquals(currentExecutionAttemptID, runningAttempt.getAttemptID());
		assertEquals(currentExecutionAttempt, runningAttempt.getAttempt());
		assertEquals(currentExecutionExpectedState, runningAttempt.getStatus());
		assertEquals(currentExecutionLocation.getHostname() + ":" + currentExecutionLocation.dataPort(), runningAttempt.getHost());
		assertEquals(currentExecutionLocation.getResourceID(), runningAttempt.getResourceID());
		assertEquals(currentExecutionDeployTimestamps, runningAttempt.getStartTime());
		assertEquals(-1, runningAttempt.getEndTime());
		assertTrue(runningAttempt.getDuration() > 0);
		assertEquals(STRINGIFIED_NULL_EXCEPTION, runningAttempt.getFailureCause());

		final SubtaskExecutionAttemptInfo failedAttempt = attemptsInfoArray[1];
		assertEquals(failedExecutionAttemptID, failedAttempt.getAttemptID());
		assertEquals(failedExecutionAttempt, failedAttempt.getAttempt());
		assertEquals(failedExecutionState, failedAttempt.getStatus());
		assertEquals(failedExecutionLocation.getHostname() + ":" + failedExecutionLocation.dataPort(), failedAttempt.getHost());
		assertEquals(failedExecutionLocation.getResourceID(), failedAttempt.getResourceID());
		assertEquals(failedExecutionDeployTimestamps, failedAttempt.getStartTime());
		assertEquals(failedExecutionFailedTimestamps, failedAttempt.getEndTime());
		assertEquals(failedExecutionFailedTimestamps - failedExecutionDeployTimestamps, failedAttempt.getDuration());
		assertEquals(failureCause, failedAttempt.getFailureCause());
	}
}

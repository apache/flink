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

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptMessageParameters;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.job.UserAccumulator;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link SubtaskExecutionAttemptAccumulatorsHandler}.
 */
public class SubtaskExecutionAttemptAccumulatorsHandlerTest extends TestLogger {

	@Test
	public void testHandleRequest() throws Exception {

		// Instance the handler.
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(new Configuration());

		final SubtaskExecutionAttemptAccumulatorsHandler handler = new SubtaskExecutionAttemptAccumulatorsHandler(
			() -> null,
			Time.milliseconds(100L),
			Collections.emptyMap(),
			SubtaskExecutionAttemptAccumulatorsHeaders.getInstance(),
			new ExecutionGraphCache(
				restHandlerConfiguration.getTimeout(),
				Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
			TestingUtils.defaultExecutor());

		// Instance a empty request.
		final HandlerRequest<EmptyRequestBody, SubtaskAttemptMessageParameters> request = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new SubtaskAttemptMessageParameters()
		);

		final Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = new HashMap<>(3);
		userAccumulators.put("IntCounter", OptionalFailure.of(new IntCounter(10)));
		userAccumulators.put("LongCounter", OptionalFailure.of(new LongCounter(100L)));
		userAccumulators.put("Failure", OptionalFailure.ofFailure(new FlinkRuntimeException("Test")));

		// Instance the expected result.
		final StringifiedAccumulatorResult[] accumulatorResults =
			StringifiedAccumulatorResult.stringifyAccumulatorResults(userAccumulators);

		final int attemptNum = 1;
		final int subtaskIndex = 2;

		// Instance the tested execution.
		final ArchivedExecution execution = new ArchivedExecution(
			accumulatorResults,
			null,
			new ExecutionAttemptID(),
			attemptNum,
			ExecutionState.FINISHED,
			null,
			null,
			null,
			subtaskIndex,
			new long[ExecutionState.values().length]);

		// Invoke tested method.
		final SubtaskExecutionAttemptAccumulatorsInfo accumulatorsInfo = handler.handleRequest(request, execution);

		final ArrayList<UserAccumulator> userAccumulatorList = new ArrayList<>(userAccumulators.size());
		for (StringifiedAccumulatorResult accumulatorResult : accumulatorResults) {
			userAccumulatorList.add(
				new UserAccumulator(
					accumulatorResult.getName(),
					accumulatorResult.getType(),
					accumulatorResult.getValue()));
		}

		final SubtaskExecutionAttemptAccumulatorsInfo expected = new SubtaskExecutionAttemptAccumulatorsInfo(
			subtaskIndex,
			attemptNum,
			execution.getAttemptId().toString(),
			userAccumulatorList);

		// Verify.
		assertEquals(expected, accumulatorsInfo);
	}
}

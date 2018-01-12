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
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptMessageParameters;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.job.UserAccumulator;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests of {@link SubtaskExecutionAttemptAccumulatorsHandler}.
 */
public class SubtaskExecutionAttemptAccumulatorsHandlerTest extends TestLogger {

	@SuppressWarnings("unchecked")
	@Test
	public void testHandleRequest() throws Exception {

		// Instance the handler.
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(new Configuration());

		final SubtaskExecutionAttemptAccumulatorsHandler handler = new SubtaskExecutionAttemptAccumulatorsHandler(
			CompletableFuture.completedFuture("127.0.0.1:9527"),
			mock(GatewayRetriever.class),
			Time.milliseconds(100),
			restHandlerConfiguration.getResponseHeaders(),
			null,
			new ExecutionGraphCache(
				restHandlerConfiguration.getTimeout(),
				Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
			mock(Executor.class)
		);

		// Instance a empty request.
		final HandlerRequest<EmptyRequestBody, SubtaskAttemptMessageParameters> request = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new SubtaskAttemptMessageParameters()
		);

		final int attemptNum = 1;
		final int subtaskIndex = 2;

		final ExecutionVertex executionVertex = mock(ExecutionVertex.class);
		when(executionVertex.getParallelSubtaskIndex()).thenReturn(subtaskIndex);

		// Instance the tested execution.
		final Execution execution = new Execution(
			mock(Executor.class),
			executionVertex,
			attemptNum,
			1L,
			System.currentTimeMillis(),
			Time.milliseconds(10000)
		);

		final Map<String, Accumulator<?, ?>> userAccumulators = new HashMap<>();
		userAccumulators.put("IntCounter", new IntCounter(10));
		userAccumulators.put("LongCounter", new LongCounter(100L));

		execution.setAccumulators(userAccumulators);

		// Invoke tested method.
		final SubtaskExecutionAttemptAccumulatorsInfo accumulatorsInfo = handler.handleRequest(request, execution);

		// Instance the expected result.
		final StringifiedAccumulatorResult[] accumulatorResults =
			StringifiedAccumulatorResult.stringifyAccumulatorResults(userAccumulators);

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
			userAccumulatorList
		);

		// Verify.
		assertEquals(expected, accumulatorsInfo);
	}
}

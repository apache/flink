/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.LogsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerLogsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link TaskManagerLogsHandler}.
 */
public class TaskManagerLogsHandlerTest extends TestLogger {

	private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();

	@Test
	public void testGetTaskManagerLogsList() throws Exception {
		final ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		final TaskManagerLogsHandler taskManagerLogsHandler = new TaskManagerLogsHandler(
			() -> CompletableFuture.completedFuture(null),
			TestingUtils.TIMEOUT(),
			Collections.emptyMap(),
			TaskManagerLogsHeaders.getInstance(),
			() -> CompletableFuture.completedFuture(resourceManagerGateway));
		final HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> handlerRequest = createRequest(EXPECTED_TASK_MANAGER_ID);
		LogsInfo logsInfo = taskManagerLogsHandler.handleRequest(handlerRequest, resourceManagerGateway).get();
		assertEquals(logsInfo.getLogInfos().size(), resourceManagerGateway.requestTaskManagerLogList(EXPECTED_TASK_MANAGER_ID, TestingUtils.TIMEOUT()).get().size());
	}

	private static HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> createRequest(ResourceID taskManagerId) throws HandlerRequestException {
		final Map<String, String> pathParameters = new HashMap<>();
		pathParameters.put(TaskManagerIdPathParameter.KEY, taskManagerId.toString());
		final Map<String, List<String>> queryParameters = new HashMap<>();

		return new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new TaskManagerMessageParameters(),
			pathParameters,
			queryParameters);
	}
}

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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerLogListHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link TaskManagerLogListHandler}.
 */
public class TaskManagerLogListHandlerTest extends TestLogger {

	private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> handlerRequest;

	private File file1;

	private File file2;

	@BeforeClass
	public static void setup() throws HandlerRequestException {

		handlerRequest = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new TaskManagerMessageParameters(),
			Collections.singletonMap(TaskManagerIdPathParameter.KEY, EXPECTED_TASK_MANAGER_ID.getResourceIdString()),
			Collections.emptyMap());
	}

	@Before
	public void setupTest() throws IOException {
		file1 = createFileWithContent(UUID.randomUUID().toString());
		file2 = createFileWithContent(UUID.randomUUID().toString());
	}

	@Test
	public void handleRequest() throws RestHandlerException, ExecutionException, InterruptedException {
		TaskManagerLogListHandler taskManagerLogListHandler = new TaskManagerLogListHandler(
			() -> new CompletableFuture<>(),
			RpcUtils.INF_TIMEOUT,
			Collections.EMPTY_MAP,
			TaskManagerLogListHeaders.getInstance(),
			() -> CompletableFuture.completedFuture(new TestingResourceManagerGateway() {
				@Override
				public CompletableFuture<String[]> requestTaskManagerLogList(ResourceID taskManagerId, @RpcTimeout Time timeout) {
					return CompletableFuture.completedFuture(new String[]{file1.getName(), file2.getName()});
				}
			}));

		CompletableFuture<LogListInfo> logListInfoCompletableFuture = taskManagerLogListHandler.handleRequest(handlerRequest, null);
		String[] filenames = new String[2];
		filenames[0] = file1.getName();
		filenames[1] = file2.getName();
		LogListInfo expected = new LogListInfo(filenames);
		LogListInfo result = logListInfoCompletableFuture.get();

		assertTrue(result.equals(expected));
	}

	private static File createFileWithContent(String fileContent) throws IOException {
		final File file = temporaryFolder.newFile();

		// write random content into the file
		try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
			fileOutputStream.write(fileContent.getBytes("UTF-8"));
		}
		return file;
	}
}

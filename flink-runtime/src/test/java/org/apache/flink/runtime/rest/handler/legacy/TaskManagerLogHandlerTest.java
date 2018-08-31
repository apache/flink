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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.router.RouteResult;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the TaskManagersLogHandler.
 */
public class TaskManagerLogHandlerTest {
	@Test
	public void testGetPaths() {
		TaskManagerLogHandler handlerLog = new TaskManagerLogHandler(
			mock(GatewayRetriever.class),
			Executors.directExecutor(),
			CompletableFuture.completedFuture("/jm/address"),
			TestingUtils.TIMEOUT(),
			TaskManagerLogHandler.FileMode.LOG,
			new Configuration());
		String[] pathsLog = handlerLog.getPaths();
		Assert.assertEquals(1, pathsLog.length);
		Assert.assertEquals("/taskmanagers/:taskmanagerid/log", pathsLog[0]);

		TaskManagerLogHandler handlerOut = new TaskManagerLogHandler(
			mock(GatewayRetriever.class),
			Executors.directExecutor(),
			CompletableFuture.completedFuture("/jm/address"),
			TestingUtils.TIMEOUT(),
			TaskManagerLogHandler.FileMode.STDOUT,
			new Configuration());
		String[] pathsOut = handlerOut.getPaths();
		Assert.assertEquals(1, pathsOut.length);
		Assert.assertEquals("/taskmanagers/:taskmanagerid/stdout", pathsOut[0]);
	}

	@Test
	public void testLogFetchingFailure() throws Exception {
		// ========= setup TaskManager =================================================================================
		InstanceID tmID = new InstanceID();
		ResourceID tmRID = new ResourceID(tmID.toString());
		TaskManagerGateway taskManagerGateway = mock(TaskManagerGateway.class);
		when(taskManagerGateway.getAddress()).thenReturn("/tm/address");

		Instance taskManager = mock(Instance.class);
		when(taskManager.getId()).thenReturn(tmID);
		when(taskManager.getTaskManagerID()).thenReturn(tmRID);
		when(taskManager.getTaskManagerGateway()).thenReturn(taskManagerGateway);
		CompletableFuture<TransientBlobKey> future = new CompletableFuture<>();
		future.completeExceptionally(new IOException("failure"));
		when(taskManagerGateway.requestTaskManagerLog(any(Time.class))).thenReturn(future);

		// ========= setup JobManager ==================================================================================

		JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		when(jobManagerGateway.requestBlobServerPort(any(Time.class))).thenReturn(CompletableFuture.completedFuture(1337));
		when(jobManagerGateway.getHostname()).thenReturn("localhost");
		when(jobManagerGateway.requestTaskManagerInstance(any(ResourceID.class), any(Time.class))).thenReturn(
			CompletableFuture.completedFuture(Optional.of(taskManager)));

		GatewayRetriever<JobManagerGateway> retriever = mock(GatewayRetriever.class);
		when(retriever.getNow())
			.thenReturn(Optional.of(jobManagerGateway));

		TaskManagerLogHandler handler = new TaskManagerLogHandler(
			retriever,
			Executors.directExecutor(),
			CompletableFuture.completedFuture("/jm/address"),
			TestingUtils.TIMEOUT(),
			TaskManagerLogHandler.FileMode.LOG,
			new Configuration());

		final AtomicReference<String> exception = new AtomicReference<>();

		ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
		when(ctx.write(isA(ByteBuf.class))).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				ByteBuf data = invocationOnMock.getArgumentAt(0, ByteBuf.class);
				exception.set(new String(data.array(), ConfigConstants.DEFAULT_CHARSET));
				return null;
			}
		});

		Map<String, String> pathParams = new HashMap<>();
		pathParams.put(TaskManagersHandler.TASK_MANAGER_ID_KEY, tmID.toString());
		RoutedRequest routedRequest = new RoutedRequest(
			new RouteResult(
				"shouldn't be used",
				"shouldn't be used either",
				pathParams,
				new HashMap<>(),
				new Object()),
			new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/taskmanagers/" + tmID + "/log"));

		handler.respondAsLeader(ctx, routedRequest, jobManagerGateway);

		Assert.assertEquals("Fetching TaskManager log failed.", exception.get());
	}
}

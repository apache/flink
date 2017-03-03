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
package org.apache.flink.runtime.webmonitor.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.router.Routed;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.webmonitor.JobManagerRetriever;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import scala.Option;
import scala.collection.JavaConverters;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future$;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class TaskManagerLogHandlerTest {
	@Test
	public void testGetPaths() {
		TaskManagerLogHandler handlerLog = new TaskManagerLogHandler(
			mock(JobManagerRetriever.class),
			mock(ExecutionContextExecutor.class),
			Future$.MODULE$.successful("/jm/address"),
			AkkaUtils.getDefaultClientTimeout(),
			TaskManagerLogHandler.FileMode.LOG,
			new Configuration(),
			false);
		String[] pathsLog = handlerLog.getPaths();
		Assert.assertEquals(1, pathsLog.length);
		Assert.assertEquals("/taskmanagers/:taskmanagerid/log", pathsLog[0]);

		TaskManagerLogHandler handlerOut = new TaskManagerLogHandler(
			mock(JobManagerRetriever.class),
			mock(ExecutionContextExecutor.class),
			Future$.MODULE$.successful("/jm/address"),
			AkkaUtils.getDefaultClientTimeout(),
			TaskManagerLogHandler.FileMode.STDOUT,
			new Configuration(),
			false);
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
		CompletableFuture<BlobKey> future = new FlinkCompletableFuture<>();
		future.completeExceptionally(new IOException("failure"));
		when(taskManagerGateway.requestTaskManagerLog(any(Time.class))).thenReturn(future);

		// ========= setup JobManager ==================================================================================

		ActorGateway jobManagerGateway = mock(ActorGateway.class);
		Object registeredTaskManagersAnswer = new JobManagerMessages.RegisteredTaskManagers(
			JavaConverters.collectionAsScalaIterableConverter(Collections.singletonList(taskManager)).asScala());

		when(jobManagerGateway.ask(isA(JobManagerMessages.RequestRegisteredTaskManagers$.class), any(FiniteDuration.class)))
			.thenReturn(Future$.MODULE$.successful(registeredTaskManagersAnswer));
		when(jobManagerGateway.ask(isA(JobManagerMessages.getRequestBlobManagerPort().getClass()), any(FiniteDuration.class)))
			.thenReturn(Future$.MODULE$.successful((Object) 5));
		when(jobManagerGateway.ask(isA(JobManagerMessages.RequestTaskManagerInstance.class), any(FiniteDuration.class)))
			.thenReturn(Future$.MODULE$.successful((Object) new JobManagerMessages.TaskManagerInstance(Option.apply(taskManager))));
		when(jobManagerGateway.path()).thenReturn("/jm/address");

		JobManagerRetriever retriever = mock(JobManagerRetriever.class);
		when(retriever.getJobManagerGatewayAndWebPort())
			.thenReturn(Option.apply(new scala.Tuple2<ActorGateway, Integer>(jobManagerGateway, 0)));


		TaskManagerLogHandler handler = new TaskManagerLogHandler(
			retriever,
			ExecutionContext$.MODULE$.fromExecutor(Executors.directExecutor()),
			Future$.MODULE$.successful("/jm/address"),
			AkkaUtils.getDefaultClientTimeout(),
			TaskManagerLogHandler.FileMode.LOG,
			new Configuration(),
			false);

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
		Routed routed = mock(Routed.class);
		when(routed.pathParams()).thenReturn(pathParams);
		when(routed.request()).thenReturn(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/taskmanagers/" + tmID + "/log"));

		handler.respondAsLeader(ctx, routed, jobManagerGateway);

		Assert.assertEquals("Fetching TaskManager log failed.", exception.get());
	}
}

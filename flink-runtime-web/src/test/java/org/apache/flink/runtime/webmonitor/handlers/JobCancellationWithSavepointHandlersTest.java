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

import akka.dispatch.ExecutionContexts$;
import akka.dispatch.Futures;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJobWithSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.junit.Assert;
import org.junit.Test;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobCancellationWithSavepointHandlersTest {

	private static final ExecutionContext EC = ExecutionContexts$.MODULE$.fromExecutor(Executors.directExecutor());

	@Test
	public void testGetPaths() {
		JobCancellationWithSavepointHandlers handler = new JobCancellationWithSavepointHandlers(mock(ExecutionGraphHolder.class), EC);

		JobCancellationWithSavepointHandlers.TriggerHandler triggerHandler = handler.getTriggerHandler();
		String[] triggerPaths = triggerHandler.getPaths();
		Assert.assertEquals(2, triggerPaths.length);
		List<String> triggerPathsList = Arrays.asList(triggerPaths);
		Assert.assertTrue(triggerPathsList.contains("/jobs/:jobid/cancel-with-savepoint"));
		Assert.assertTrue(triggerPathsList.contains("/jobs/:jobid/cancel-with-savepoint/target-directory/:targetDirectory"));

		JobCancellationWithSavepointHandlers.InProgressHandler progressHandler = handler.getInProgressHandler();
		String[] progressPaths = progressHandler.getPaths();
		Assert.assertEquals(1, progressPaths.length);
		Assert.assertEquals("/jobs/:jobid/cancel-with-savepoint/in-progress/:requestId", progressPaths[0]);
	}

	/**
	 * Tests that the cancellation ask timeout respects the checkpoint timeout.
	 * Otherwise, AskTimeoutExceptions are bound to happen for large state.
	 */
	@Test
	public void testAskTimeoutEqualsCheckpointTimeout() throws Exception {
		long timeout = 128288238L;
		JobID jobId = new JobID();
		ExecutionGraphHolder holder = mock(ExecutionGraphHolder.class);
		ExecutionGraph graph = mock(ExecutionGraph.class);
		CheckpointCoordinator coord = mock(CheckpointCoordinator.class);
		when(holder.getExecutionGraph(eq(jobId), any(ActorGateway.class))).thenReturn(graph);
		when(graph.getCheckpointCoordinator()).thenReturn(coord);
		when(coord.getCheckpointTimeout()).thenReturn(timeout);

		JobCancellationWithSavepointHandlers handlers = new JobCancellationWithSavepointHandlers(holder, EC);
		JobCancellationWithSavepointHandlers.TriggerHandler handler = handlers.getTriggerHandler();

		Map<String, String> params = new HashMap<>();
		params.put("jobid", jobId.toString());
		params.put("targetDirectory", "placeholder");

		ActorGateway jobManager = mock(ActorGateway.class);

		Future<Object> future = Futures.successful((Object) new CancellationSuccess(jobId, null));
		when(jobManager.ask(any(Object.class), any(FiniteDuration.class))).thenReturn(future);

		handler.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);

		verify(jobManager).ask(any(CancelJobWithSavepoint.class), eq(FiniteDuration.apply(timeout, "ms")));
	}

	/**
	 * Tests that the savepoint directory configuration is respected.
	 */
	@Test
	public void testSavepointDirectoryConfiguration() throws Exception {
		long timeout = 128288238L;
		JobID jobId = new JobID();
		ExecutionGraphHolder holder = mock(ExecutionGraphHolder.class);
		ExecutionGraph graph = mock(ExecutionGraph.class);
		CheckpointCoordinator coord = mock(CheckpointCoordinator.class);
		when(holder.getExecutionGraph(eq(jobId), any(ActorGateway.class))).thenReturn(graph);
		when(graph.getCheckpointCoordinator()).thenReturn(coord);
		when(coord.getCheckpointTimeout()).thenReturn(timeout);

		JobCancellationWithSavepointHandlers handlers = new JobCancellationWithSavepointHandlers(holder, EC, "the-default-directory");
		JobCancellationWithSavepointHandlers.TriggerHandler handler = handlers.getTriggerHandler();

		Map<String, String> params = new HashMap<>();
		params.put("jobid", jobId.toString());

		ActorGateway jobManager = mock(ActorGateway.class);

		Future<Object> future = Futures.successful((Object) new CancellationSuccess(jobId, null));
		when(jobManager.ask(any(Object.class), any(FiniteDuration.class))).thenReturn(future);

		// 1. Use targetDirectory path param
		params.put("targetDirectory", "custom-directory");
		handler.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);

		verify(jobManager).ask(eq(new CancelJobWithSavepoint(jobId, "custom-directory")), eq(FiniteDuration.apply(timeout, "ms")));

		// 2. Use default
		params.remove("targetDirectory");

		handler.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);

		verify(jobManager).ask(eq(new CancelJobWithSavepoint(jobId, "the-default-directory")), eq(FiniteDuration.apply(timeout, "ms")));

		// 3. Throw Exception
		handlers = new JobCancellationWithSavepointHandlers(holder, EC, null);
		handler = handlers.getTriggerHandler();

		try {
			handler.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);
			fail("Did not throw expected test Exception");
		} catch (Exception e) {
			IllegalStateException cause = (IllegalStateException) e.getCause();
			assertEquals(true, cause.getMessage().contains(ConfigConstants.SAVEPOINT_DIRECTORY_KEY));
		}
	}

	/**
	 * Tests triggering a new request and monitoring it.
	 */
	@Test
	public void testTriggerNewRequest() throws Exception {
		JobID jobId = new JobID();
		ExecutionGraphHolder holder = mock(ExecutionGraphHolder.class);
		ExecutionGraph graph = mock(ExecutionGraph.class);
		CheckpointCoordinator coord = mock(CheckpointCoordinator.class);
		when(holder.getExecutionGraph(eq(jobId), any(ActorGateway.class))).thenReturn(graph);
		when(graph.getCheckpointCoordinator()).thenReturn(coord);

		JobCancellationWithSavepointHandlers handlers = new JobCancellationWithSavepointHandlers(holder, EC);
		JobCancellationWithSavepointHandlers.TriggerHandler trigger = handlers.getTriggerHandler();
		JobCancellationWithSavepointHandlers.InProgressHandler progress = handlers.getInProgressHandler();

		Map<String, String> params = new HashMap<>();
		params.put("jobid", jobId.toString());
		params.put("targetDirectory", "custom-directory");

		ActorGateway jobManager = mock(ActorGateway.class);

		// Successful
		Promise<Object> promise = new Promise.DefaultPromise<>();
		when(jobManager.ask(any(Object.class), any(FiniteDuration.class))).thenReturn(promise);

		// Trigger
		FullHttpResponse response = trigger.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);

		verify(jobManager).ask(eq(new CancelJobWithSavepoint(jobId, "custom-directory")), any(FiniteDuration.class));

		String location = String.format("/jobs/%s/cancel-with-savepoint/in-progress/1", jobId);

		assertEquals(HttpResponseStatus.ACCEPTED, response.getStatus());
		assertEquals("application/json", response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
		assertEquals(Integer.toString(response.content().readableBytes()), response.headers().get(HttpHeaders.Names.CONTENT_LENGTH));
		assertEquals(location, response.headers().get(HttpHeaders.Names.LOCATION));

		String json = response.content().toString(Charset.forName("UTF-8"));
		JsonNode root = new ObjectMapper().readTree(json);

		assertEquals("accepted", root.get("status").asText());
		assertEquals("1", root.get("request-id").asText());
		assertEquals(location, root.get("location").asText());

		// Trigger again
		response = trigger.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);
		assertEquals(HttpResponseStatus.ACCEPTED, response.getStatus());
		assertEquals("application/json", response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
		assertEquals(Integer.toString(response.content().readableBytes()), response.headers().get(HttpHeaders.Names.CONTENT_LENGTH));
		assertEquals(location, response.headers().get(HttpHeaders.Names.LOCATION));

		json = response.content().toString(Charset.forName("UTF-8"));
		root = new ObjectMapper().readTree(json);

		assertEquals("accepted", root.get("status").asText());
		assertEquals("1", root.get("request-id").asText());
		assertEquals(location, root.get("location").asText());

		// Only single actual request
		verify(jobManager).ask(eq(new CancelJobWithSavepoint(jobId, "custom-directory")), any(FiniteDuration.class));

		// Query progress
		params.put("requestId", "1");

		response = progress.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);
		assertEquals(HttpResponseStatus.ACCEPTED, response.getStatus());
		assertEquals("application/json", response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
		assertEquals(Integer.toString(response.content().readableBytes()), response.headers().get(HttpHeaders.Names.CONTENT_LENGTH));

		json = response.content().toString(Charset.forName("UTF-8"));
		root = new ObjectMapper().readTree(json);

		assertEquals("in-progress", root.get("status").asText());
		assertEquals("1", root.get("request-id").asText());

		// Complete
		promise.success(new CancellationSuccess(jobId, "_path-savepoint_"));

		response = progress.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);

		assertEquals(HttpResponseStatus.CREATED, response.getStatus());
		assertEquals("application/json", response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
		assertEquals(Integer.toString(response.content().readableBytes()), response.headers().get(HttpHeaders.Names.CONTENT_LENGTH));

		json = response.content().toString(Charset.forName("UTF-8"));

		root = new ObjectMapper().readTree(json);

		assertEquals("success", root.get("status").asText());
		assertEquals("1", root.get("request-id").asText());
		assertEquals("_path-savepoint_", root.get("savepoint-path").asText());

		// Query again, keep recent history

		response = progress.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);

		assertEquals(HttpResponseStatus.CREATED, response.getStatus());
		assertEquals("application/json", response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
		assertEquals(Integer.toString(response.content().readableBytes()), response.headers().get(HttpHeaders.Names.CONTENT_LENGTH));

		json = response.content().toString(Charset.forName("UTF-8"));

		root = new ObjectMapper().readTree(json);

		assertEquals("success", root.get("status").asText());
		assertEquals("1", root.get("request-id").asText());
		assertEquals("_path-savepoint_", root.get("savepoint-path").asText());

		// Query for unknown request
		params.put("requestId", "9929");

		response = progress.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);
		assertEquals(HttpResponseStatus.BAD_REQUEST, response.getStatus());
		assertEquals("application/json", response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
		assertEquals(Integer.toString(response.content().readableBytes()), response.headers().get(HttpHeaders.Names.CONTENT_LENGTH));

		json = response.content().toString(Charset.forName("UTF-8"));

		root = new ObjectMapper().readTree(json);

		assertEquals("failed", root.get("status").asText());
		assertEquals("9929", root.get("request-id").asText());
		assertEquals("Unknown job/request ID", root.get("cause").asText());
	}

	/**
	 * Tests response when a request fails.
	 */
	@Test
	public void testFailedCancellation() throws Exception {
		JobID jobId = new JobID();
		ExecutionGraphHolder holder = mock(ExecutionGraphHolder.class);
		ExecutionGraph graph = mock(ExecutionGraph.class);
		CheckpointCoordinator coord = mock(CheckpointCoordinator.class);
		when(holder.getExecutionGraph(eq(jobId), any(ActorGateway.class))).thenReturn(graph);
		when(graph.getCheckpointCoordinator()).thenReturn(coord);

		JobCancellationWithSavepointHandlers handlers = new JobCancellationWithSavepointHandlers(holder, EC);
		JobCancellationWithSavepointHandlers.TriggerHandler trigger = handlers.getTriggerHandler();
		JobCancellationWithSavepointHandlers.InProgressHandler progress = handlers.getInProgressHandler();

		Map<String, String> params = new HashMap<>();
		params.put("jobid", jobId.toString());
		params.put("targetDirectory", "custom-directory");

		ActorGateway jobManager = mock(ActorGateway.class);

		// Successful
		Future<Object> future = Futures.failed(new Exception("Test Exception"));
		when(jobManager.ask(any(Object.class), any(FiniteDuration.class))).thenReturn(future);

		// Trigger
		trigger.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);
		verify(jobManager).ask(eq(new CancelJobWithSavepoint(jobId, "custom-directory")), any(FiniteDuration.class));

		// Query progress
		params.put("requestId", "1");

		FullHttpResponse response = progress.handleRequest(params, Collections.<String, String>emptyMap(), jobManager);
		assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.getStatus());
		assertEquals("application/json", response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
		assertEquals(Integer.toString(response.content().readableBytes()), response.headers().get(HttpHeaders.Names.CONTENT_LENGTH));

		String json = response.content().toString(Charset.forName("UTF-8"));
		JsonNode root = new ObjectMapper().readTree(json);

		assertEquals("failed", root.get("status").asText());
		assertEquals("1", root.get("request-id").asText());
		assertEquals("Test Exception", root.get("cause").asText());
	}
}

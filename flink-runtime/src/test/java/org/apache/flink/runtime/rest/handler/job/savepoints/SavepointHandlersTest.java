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

package org.apache.flink.runtime.rest.handler.job.savepoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.SavepointTriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Test for {@link SavepointHandlers}.
 */
public class SavepointHandlersTest extends TestLogger {

	private static final CompletableFuture<String> LOCAL_REST_ADDRESS =
		CompletableFuture.completedFuture("localhost:12345");

	private static final Time TIMEOUT = Time.seconds(10);

	private static final JobID JOB_ID = new JobID();

	private static final String COMPLETED_SAVEPOINT_EXTERNAL_POINTER = "/tmp/savepoint-0d2fb9-8d5e0106041a";

	private static final String DEFAULT_REQUESTED_SAVEPOINT_TARGET_DIRECTORY = "/tmp";

	@Mock
	private RestfulGateway mockRestfulGateway;

	private SavepointHandlers.SavepointTriggerHandler savepointTriggerHandler;

	private SavepointHandlers.SavepointStatusHandler savepointStatusHandler;

	private GatewayRetriever<RestfulGateway> leaderRetriever;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);

		leaderRetriever = () -> CompletableFuture.completedFuture(mockRestfulGateway);

		final SavepointHandlers savepointHandlers = new SavepointHandlers(null);
		savepointTriggerHandler = savepointHandlers.new SavepointTriggerHandler(
			LOCAL_REST_ADDRESS,
			leaderRetriever,
			TIMEOUT,
			Collections.emptyMap());

		savepointStatusHandler = savepointHandlers.new SavepointStatusHandler(
			LOCAL_REST_ADDRESS,
			leaderRetriever,
			TIMEOUT,
			Collections.emptyMap());
	}

	@Test
	public void testSavepointCompletedSuccessfully() throws Exception {
		final CompletableFuture<String> savepointLocationFuture =
			new CompletableFuture<>();
		when(mockRestfulGateway.triggerSavepoint(any(JobID.class), anyString(), any(Time.class)))
			.thenReturn(savepointLocationFuture);

		final SavepointTriggerId savepointTriggerId = savepointTriggerHandler.handleRequest(
			triggerSavepointRequest(),
			mockRestfulGateway).get().getSavepointTriggerId();

		SavepointResponseBody savepointResponseBody;
		savepointResponseBody = savepointStatusHandler.handleRequest(
			savepointStatusRequest(savepointTriggerId),
			mockRestfulGateway).get();

		assertThat(
			savepointResponseBody.getStatus().getId(),
			equalTo(QueueStatus.Id.IN_PROGRESS));

		savepointLocationFuture.complete(COMPLETED_SAVEPOINT_EXTERNAL_POINTER);
		savepointResponseBody = savepointStatusHandler.handleRequest(
			savepointStatusRequest(savepointTriggerId),
			mockRestfulGateway).get();

		assertThat(
			savepointResponseBody.getStatus().getId(),
			equalTo(QueueStatus.Id.COMPLETED));
		assertThat(savepointResponseBody.getSavepoint(), notNullValue());
		assertThat(
			savepointResponseBody.getSavepoint().getLocation(),
			equalTo(COMPLETED_SAVEPOINT_EXTERNAL_POINTER));
	}

	@Test
	public void testTriggerSavepointWithDefaultDirectory() throws Exception {
		final ArgumentCaptor<String> targetDirectoryCaptor = ArgumentCaptor.forClass(String.class);
		when(mockRestfulGateway.triggerSavepoint(any(JobID.class), targetDirectoryCaptor.capture(), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(COMPLETED_SAVEPOINT_EXTERNAL_POINTER));
		final String defaultSavepointDir = "/other/dir";
		final SavepointHandlers savepointHandlers = new SavepointHandlers(defaultSavepointDir);
		final SavepointHandlers.SavepointTriggerHandler savepointTriggerHandler = savepointHandlers.new SavepointTriggerHandler(
			LOCAL_REST_ADDRESS,
			leaderRetriever,
			TIMEOUT,
			Collections.emptyMap());

		savepointTriggerHandler.handleRequest(
			triggerSavepointRequestWithDefaultDirectory(),
			mockRestfulGateway).get();

		final List<String> targetDirectories = targetDirectoryCaptor.getAllValues();
		assertThat(targetDirectories, hasItem(equalTo(defaultSavepointDir)));
	}

	@Test
	public void testGetSavepointStatusWithUnknownSavepointTriggerId() throws Exception {
		final SavepointTriggerId unknownSavepointTriggerId = new SavepointTriggerId();

		try {
			savepointStatusHandler.handleRequest(
				savepointStatusRequest(unknownSavepointTriggerId),
				mockRestfulGateway).get();
			fail("Expected exception not thrown.");
		} catch (ExecutionException e) {
			final Throwable cause = e.getCause();
			assertThat(cause, instanceOf(RestHandlerException.class));
			final RestHandlerException restHandlerException = (RestHandlerException) cause;
			assertThat(restHandlerException.getMessage(), containsString("Savepoint not found"));
			assertThat(restHandlerException.getHttpResponseStatus(), equalTo(HttpResponseStatus.NOT_FOUND));
		}
	}

	@Test
	public void testTriggerSavepointNoDirectory() throws Exception {
		when(mockRestfulGateway.triggerSavepoint(any(JobID.class), anyString(), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(COMPLETED_SAVEPOINT_EXTERNAL_POINTER));

		try {
			savepointTriggerHandler.handleRequest(
				triggerSavepointRequestWithDefaultDirectory(),
				mockRestfulGateway).get();
			fail("Expected exception not thrown.");
		} catch (ExecutionException e) {
			final Throwable cause = e.getCause();
			assertThat(cause, instanceOf(RestHandlerException.class));
			final RestHandlerException restHandlerException = (RestHandlerException) cause;
			assertThat(
				restHandlerException.getMessage(),
				equalTo("Config key [state.savepoints.dir] is not set. " +
					"Property [target-directory] must be provided."));
			assertThat(restHandlerException.getHttpResponseStatus(), equalTo(HttpResponseStatus.BAD_REQUEST));
		}
	}

	@Test
	public void testSavepointCompletedWithException() throws Exception {
		when(mockRestfulGateway.triggerSavepoint(any(JobID.class), anyString(), any(Time.class)))
			.thenReturn(FutureUtils.completedExceptionally(new RuntimeException("expected")));

		final SavepointTriggerId savepointTriggerId = savepointTriggerHandler.handleRequest(
			triggerSavepointRequest(),
			mockRestfulGateway).get().getSavepointTriggerId();

		final SavepointResponseBody savepointResponseBody = savepointStatusHandler.handleRequest(
			savepointStatusRequest(savepointTriggerId),
			mockRestfulGateway).get();

		assertThat(savepointResponseBody.getStatus().getId(), equalTo(QueueStatus.Id.COMPLETED));
		assertThat(savepointResponseBody.getSavepoint(), notNullValue());
		assertThat(savepointResponseBody.getSavepoint().getFailureCause(), notNullValue());
		final Throwable savepointError = savepointResponseBody.getSavepoint()
			.getFailureCause()
			.deserializeError(ClassLoader.getSystemClassLoader());
		assertThat(savepointError.getMessage(), equalTo("expected"));
		assertThat(savepointError, instanceOf(RuntimeException.class));
	}

	private static HandlerRequest<SavepointTriggerRequestBody, SavepointTriggerMessageParameters> triggerSavepointRequest() throws HandlerRequestException {
		return triggerSavepointRequest(DEFAULT_REQUESTED_SAVEPOINT_TARGET_DIRECTORY);
	}

	private static HandlerRequest<SavepointTriggerRequestBody, SavepointTriggerMessageParameters> triggerSavepointRequestWithDefaultDirectory() throws HandlerRequestException {
		return triggerSavepointRequest(null);
	}

	private static HandlerRequest<SavepointTriggerRequestBody, SavepointTriggerMessageParameters> triggerSavepointRequest(
			final String targetDirectory
	) throws HandlerRequestException {
		return new HandlerRequest<>(
			new SavepointTriggerRequestBody(targetDirectory),
			new SavepointTriggerMessageParameters(),
			Collections.singletonMap(JobIDPathParameter.KEY, JOB_ID.toString()),
			Collections.emptyMap());
	}

	private static HandlerRequest<EmptyRequestBody, SavepointStatusMessageParameters> savepointStatusRequest(
			final SavepointTriggerId savepointTriggerId) throws HandlerRequestException {
			final Map<String, String> pathParameters = new HashMap<>();
		pathParameters.put(JobIDPathParameter.KEY, JOB_ID.toString());
		pathParameters.put(SavepointTriggerIdPathParameter.KEY, savepointTriggerId.toString());

		return new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new SavepointStatusMessageParameters(),
			pathParameters,
			Collections.emptyMap());
	}

}

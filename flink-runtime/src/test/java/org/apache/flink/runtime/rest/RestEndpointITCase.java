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

package org.apache.flink.runtime.rest;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyRequest;
import org.apache.flink.runtime.rest.messages.EmptyResponse;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * IT cases for {@link RestClient} and {@link RestServerEndpoint}.
 */
public class RestEndpointITCase extends TestLogger {

	private static final JobID PATH_JOB_ID = new JobID();
	private static final JobID QUERY_JOB_ID = new JobID();
	private static final String JOB_ID_KEY = "jobid";
	private static final Time timeout = Time.seconds(10L);

	private static RestServerEndpointConfiguration serverConfig;
	private static TestRestServerEndpoint serverEndpoint;
	private static RestClient clientEndpoint;

	@BeforeClass
	public static void startEndpoints() throws ConfigurationException {
		Configuration config = new Configuration();

		serverConfig = RestServerEndpointConfiguration.fromConfiguration(config);
		RestClientConfiguration clientConfig = RestClientConfiguration.fromConfiguration(config);

		serverEndpoint = new TestRestServerEndpoint(serverConfig);
		clientEndpoint = new RestClient(clientConfig, TestingUtils.defaultExecutor());

		serverEndpoint.start();
	}

	@AfterClass
	public static void shutdownEndpoints() {
		if (serverEndpoint != null) {
			serverEndpoint.shutdown(timeout);
		}
		if (clientEndpoint != null) {
			clientEndpoint.shutdown(timeout);
		}
	}

	@Test
	public void testInterleavingRequests() throws ConfigurationException, IOException, InterruptedException, ExecutionException {
		TestParameters parameters = new TestParameters();
		parameters.jobIDPathParameter.resolve(PATH_JOB_ID);
		parameters.jobIDQueryParameter.resolve(Collections.singletonList(QUERY_JOB_ID));

		// send first request and wait until the handler blocks
		CompletableFuture<TestResponse> response1;
		synchronized (InterleavingTestHandler.LOCK) {
			response1 = clientEndpoint.sendRequest(
				serverConfig.getEndpointBindAddress(),
				serverConfig.getEndpointBindPort(),
				new InterleavingTestHeaders(),
				parameters,
				new TestRequest(1));
			InterleavingTestHandler.LOCK.wait();
		}

		// send second request and verify response
		CompletableFuture<TestResponse> response2 = clientEndpoint.sendRequest(
			serverConfig.getEndpointBindAddress(),
			serverConfig.getEndpointBindPort(),
			new InterleavingTestHeaders(),
			parameters,
			new TestRequest(2));
		Assert.assertEquals(2, response2.get().id);

		// wake up blocked handler
		synchronized (InterleavingTestHandler.LOCK) {
			InterleavingTestHandler.LOCK.notifyAll();
		}
		// verify response to first request
		Assert.assertEquals(1, response1.get().id);
	}

	@Test
	public void testVersioning() throws IOException, ExecutionException, InterruptedException, TimeoutException {
		EmptyParameters invalidVersionParameters = new EmptyParameters();
		invalidVersionParameters.versionPathParameter.resolve(RestAPIVersion.V0);

		CompletableFuture<EmptyResponse> invalidVersionResponse = clientEndpoint.sendRequest(
			serverConfig.getEndpointBindAddress(),
			serverConfig.getEndpointBindPort(),
			new VersionTestHeaders(),
			invalidVersionParameters,
			new EmptyRequest()
		);

		try {
			invalidVersionResponse.get(5, TimeUnit.SECONDS);
			Assert.fail();
		} catch (ExecutionException ee) {
			RestClientException rce = (RestClientException) ee.getCause();
			Assert.assertEquals("[Not Found.]", rce.getMessage());
		}

		EmptyParameters unspecifiedVersionParameters = new EmptyParameters();

		CompletableFuture<EmptyResponse> unspecifiedVersionResponse = clientEndpoint.sendRequest(
			serverConfig.getEndpointBindAddress(),
			serverConfig.getEndpointBindPort(),
			new VersionTestHeaders(),
			unspecifiedVersionParameters,
			new EmptyRequest()
		);

		unspecifiedVersionResponse.get(5, TimeUnit.SECONDS);
	}

	private static class TestRestServerEndpoint extends RestServerEndpoint {

		TestRestServerEndpoint(RestServerEndpointConfiguration configuration) {
			super(configuration);
		}

		@Override
		protected Collection<AbstractRestHandler<?, ?, ?>> initializeHandlers() {
			List<AbstractRestHandler<?, ?, ?>> handlers = new ArrayList<>();
			handlers.add(new InterleavingTestHandler());
			handlers.add(new TestHandler());
			return Collections.unmodifiableList(handlers);
		}
	}

	private static class TestHandler extends AbstractRestHandler<EmptyRequest, EmptyResponse, EmptyParameters> {

		TestHandler() {
			super(new VersionTestHeaders());
		}

		@Override
		protected CompletableFuture<EmptyResponse> handleRequest(@Nonnull HandlerRequest<EmptyRequest, EmptyParameters> request) throws RestHandlerException {
			return CompletableFuture.completedFuture(new EmptyResponse());
		}
	}

	private static class VersionTestHeaders implements MessageHeaders<EmptyRequest, EmptyResponse, EmptyParameters> {

		@Override
		public Class<EmptyRequest> getRequestClass() {
			return EmptyRequest.class;
		}

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.GET;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "/test/versioning";
		}

		@Override
		public Class<EmptyResponse> getResponseClass() {
			return EmptyResponse.class;
		}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public EmptyParameters getUnresolvedMessageParameters() {
			return new EmptyParameters();
		}

		@Override
		public Collection<RestAPIVersion> getSupportedAPIVersions() {
			return Collections.singleton(RestAPIVersion.V1);
		}
	}

	private static class InterleavingTestHandler extends AbstractRestHandler<TestRequest, TestResponse, TestParameters> {

		public static final Object LOCK = new Object();

		InterleavingTestHandler() {
			super(new InterleavingTestHeaders());
		}

		@Override
		protected CompletableFuture<TestResponse> handleRequest(@Nonnull HandlerRequest<TestRequest, TestParameters> request) throws RestHandlerException {
			Assert.assertEquals(request.getPathParameter(JobIDPathParameter.class), PATH_JOB_ID);
			Assert.assertEquals(request.getQueryParameter(JobIDQueryParameter.class).get(0), QUERY_JOB_ID);

			if (request.getRequestBody().id == 1) {
				synchronized (LOCK) {
					try {
						LOCK.notifyAll();
						LOCK.wait();
					} catch (InterruptedException ignored) {
					}
				}
			}
			return CompletableFuture.completedFuture(new TestResponse(request.getRequestBody().id));
		}
	}

	private static class InterleavingTestHeaders implements MessageHeaders<TestRequest, TestResponse, TestParameters> {

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.POST;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "/test/interleaving/:jobid";
		}

		@Override
		public Class<TestRequest> getRequestClass() {
			return TestRequest.class;
		}

		@Override
		public Class<TestResponse> getResponseClass() {
			return TestResponse.class;
		}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public TestParameters getUnresolvedMessageParameters() {
			return new TestParameters();
		}

		@Override
		public Collection<RestAPIVersion> getSupportedAPIVersions() {
			return Collections.singleton(RestAPIVersion.V1);
		}
	}

	private static class TestRequest implements RequestBody {
		public final int id;

		@JsonCreator
		public TestRequest(@JsonProperty("id") int id) {
			this.id = id;
		}
	}

	private static class TestResponse implements ResponseBody {
		public final int id;

		@JsonCreator
		public TestResponse(@JsonProperty("id") int id) {
			this.id = id;
		}
	}

	private static class TestParameters extends MessageParameters {
		private final JobIDPathParameter jobIDPathParameter = new JobIDPathParameter();
		private final JobIDQueryParameter jobIDQueryParameter = new JobIDQueryParameter();

		@Override
		protected void addPathParameters(Collection<MessagePathParameter<?>> pathParameters) {
			pathParameters.add(jobIDPathParameter);
		}

		@Override
		protected void addQueryParameters(Collection<MessageQueryParameter<?>> queryParameters) {
			queryParameters.add(jobIDQueryParameter);
		}
	}

	static class JobIDPathParameter extends MessagePathParameter<JobID> {
		JobIDPathParameter() {
			super(JOB_ID_KEY);
		}

		@Override
		public JobID convertFromString(String value) {
			return JobID.fromHexString(value);
		}

		@Override
		protected String convertToString(JobID value) {
			return value.toString();
		}
	}

	static class JobIDQueryParameter extends MessageQueryParameter<JobID> {
		JobIDQueryParameter() {
			super(JOB_ID_KEY, MessageParameterRequisiteness.MANDATORY);
		}

		@Override
		public JobID convertValueFromString(String value) {
			return JobID.fromHexString(value);
		}

		@Override
		public String convertStringToValue(JobID value) {
			return value.toString();
		}
	}
}

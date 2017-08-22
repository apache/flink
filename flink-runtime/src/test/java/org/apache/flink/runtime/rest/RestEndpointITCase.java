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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerResponse;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.Parameter;
import org.apache.flink.runtime.rest.messages.ParameterMapper;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * IT cases for {@link RestClientEndpoint} and {@link RestServerEndpoint}.
 */
public class RestEndpointITCase {

	private static final String PATH_JOB_ID = "1234";
	private static final String QUERY_JOB_ID = "6789";

	@Test
	public void testEndpoints() throws ConfigurationException, IOException, InterruptedException, ExecutionException {
		Configuration config = new Configuration();

		RestServerEndpointConfiguration serverConfig = RestServerEndpointConfiguration.fromConfiguration(config);
		RestClientEndpointConfiguration clientConfig = RestClientEndpointConfiguration.fromConfiguration(config);

		RestServerEndpoint serverEndpoint = new TestRestServerEndpoint(serverConfig);
		RestClientEndpoint clientEndpoint = new TestRestClientEndpoint(clientConfig);

		try {
			serverEndpoint.start();
			clientEndpoint.start();

			clientEndpoint.sendRequest(new TestHeaders(), new TestParameterMapper(), new TestRequest()).get();
			clientEndpoint.sendRequest(new TestHeaders(), new TestParameterMapper(), new TestRequest()).get();
		} catch (Exception e) {
			clientEndpoint.shutdown();
			serverEndpoint.shutdown();
			throw e;
		}
	}

	private static class TestRestServerEndpoint extends RestServerEndpoint {

		TestRestServerEndpoint(RestServerEndpointConfiguration configuration) {
			super(configuration);
		}

		@Override
		protected Collection<AbstractRestHandler<?, ?>> initializeHandlers() {
			return Collections.singleton(new TestHandler());
		}
	}

	private static class TestHandler extends AbstractRestHandler<TestRequest, TestResponse> {

		TestHandler() {
			super(new TestHeaders());
		}

		@Override
		protected CompletableFuture<HandlerResponse<TestResponse>> handleRequest(@Nonnull HandlerRequest<TestRequest> request) {
			if (!request.getPathParameters().containsKey(Parameter.JOB_ID.getKey())) {
				return CompletableFuture.completedFuture(HandlerResponse.error("Path parameter was missing.", HttpResponseStatus.INTERNAL_SERVER_ERROR));
			}
			if (!request.getQueryParameters().containsKey(Parameter.JOB_ID.getKey())) {
				return CompletableFuture.completedFuture(HandlerResponse.error("Query parameter was missing.", HttpResponseStatus.INTERNAL_SERVER_ERROR));
			}
			return CompletableFuture.completedFuture(HandlerResponse.successful(new TestResponse()));
		}
	}

	private static class TestRestClientEndpoint extends RestClientEndpoint {

		TestRestClientEndpoint(RestClientEndpointConfiguration configuration) {
			super(configuration);
		}
	}

	private static class TestRequest implements RequestBody {
	}

	private static class TestResponse implements ResponseBody {
	}

	static class TestParameterMapper extends ParameterMapper {

		public Map<Parameter, String> mapQueryParameters(Set<Parameter> queryParameters) {
			Map<Parameter, String> map = new HashMap<>(1);
			queryParameters.forEach((parameter -> map.put(parameter, QUERY_JOB_ID)));
			return map;
		}

		public Map<Parameter, String> mapPathParameters(Set<Parameter> pathParameters) {
			Map<Parameter, String> map = new HashMap<>(1);
			pathParameters.forEach((parameter -> map.put(parameter, PATH_JOB_ID)));
			return map;
		}
	}

	private static class TestHeaders implements MessageHeaders<TestRequest, TestResponse, TestParameterMapper> {

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.GET;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "/test/:jobid";
		}

		@Override
		public Set<Parameter> getPathParameters() {
			return Collections.singleton(Parameter.JOB_ID);
		}

		@Override
		public Set<Parameter> getQueryParameters() {
			return Collections.singleton(Parameter.JOB_ID);
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
	}
}

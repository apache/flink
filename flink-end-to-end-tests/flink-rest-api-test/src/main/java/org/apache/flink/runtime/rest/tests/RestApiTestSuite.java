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

package org.apache.flink.runtime.rest.tests;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import scala.concurrent.duration.FiniteDuration;

/**
 * Rest API test suite.
 */
public class RestApiTestSuite {
	private static final FiniteDuration TEST_TIMEOUT = new FiniteDuration(10L, TimeUnit.SECONDS);
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static int testSuccessCount = 0;
	private static int testFailureCount = 0;
	private static int testSkipCount = 0;

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String host = params.get("host", "localhost");
		final int port = params.getInt("port", 8081);
		final HttpTestClient httpClient = new HttpTestClient(host, port);

		// Validate Flink cluster is running
		JobIdsWithStatusOverview jobOverview = getJobOverview(httpClient);

		// Get necessary parameters for testing GET endpoints
		Map<String, String> parameterMap = getParameterMaps(httpClient, jobOverview);

		// Get list of endpoints
		List<MessageHeaders> specs = new E2ETestDispatcherRestEndpoint().getSpecs();
		specs.forEach(spec -> testMonitoringEndpointSpecs(httpClient, spec, parameterMap));

		if (testFailureCount != 0) {
			throw new RuntimeException("There are test failures. Success: " + testSuccessCount +
				" Failures: " + testFailureCount + " Skipped: " + testSkipCount);
		}
	}

	@SuppressWarnings("ConstantConditions")
	private static Map<String, String> getParameterMaps(HttpTestClient httpClient,
		JobIdsWithStatusOverview jobOverview) throws InterruptedException, IOException, TimeoutException {
		// Get necessary parameters used for all REST API testings.
		final Map<String, String> parameterMap = new HashMap<>();
		Preconditions.checkState(jobOverview.getJobsWithStatus().stream()
			.filter(jobIdWithStatus -> jobIdWithStatus.getJobStatus() == JobStatus.RUNNING)
			.count() >= 1, "Cannot found active running jobs, discontinuing test!");
		String jobId = jobOverview.getJobsWithStatus().stream()
			.filter(jobIdWithStatus -> jobIdWithStatus.getJobStatus() == JobStatus.RUNNING)
			.findFirst().get().getJobId().toString();
		parameterMap.put(":jobid", jobId);

		JobDetailsInfo jobDetailsInfo = getJobDetailInfo(httpClient, jobId);
		String vertexId = jobDetailsInfo.getJobVertexInfos().stream()
			.findFirst().get().getJobVertexID().toString();
		parameterMap.put(":vertexid", vertexId);
		parameterMap.put(":checkpointid", "1"); // test first checkpoint
		parameterMap.put(":subtaskindex", "0"); // test first subtask

		TaskManagersInfo taskManagersInfo = getTaskManagers(httpClient);
		String taskMgrId = taskManagersInfo.getTaskManagerInfos().stream().findFirst().get().getResourceId().toString();
		parameterMap.put(":taskmanagerid", taskMgrId);
		parameterMap.put(":triggerid", "");

		return parameterMap;
	}

	private static JobIdsWithStatusOverview getJobOverview(HttpTestClient httpClient)
		throws TimeoutException, InterruptedException, IOException {
		httpClient.sendGetRequest("/jobs", TEST_TIMEOUT);
		HttpTestClient.SimpleHttpResponse resp = httpClient.getNextResponse();
		Preconditions.checkState(resp.getStatus().code() == 200,
			"Cannot fetch Flink cluster status!");
		return MAPPER.readValue(resp.getContent(), JobIdsWithStatusOverview.class);
	}

	private static JobDetailsInfo getJobDetailInfo(HttpTestClient httpClient, String jobId)
		throws TimeoutException, InterruptedException, IOException {
			httpClient.sendGetRequest("/jobs/" + jobId, TEST_TIMEOUT);
			HttpTestClient.SimpleHttpResponse resp = httpClient.getNextResponse();
			Preconditions.checkState(resp.getStatus().code() == 200,
				"Cannot fetch job detail information for job " + jobId);
			return MAPPER.readValue(resp.getContent(), JobDetailsInfo.class);
	}

	private static TaskManagersInfo getTaskManagers(HttpTestClient httpClient)
		throws TimeoutException, InterruptedException, IOException {
		httpClient.sendGetRequest("/taskmanagers", TEST_TIMEOUT);
		HttpTestClient.SimpleHttpResponse resp = httpClient.getNextResponse();
		Preconditions.checkState(resp.getStatus().code() == 200,
			"Cannot fetch task manager status!");
		return MAPPER.readValue(resp.getContent(), TaskManagersInfo.class);
	}

	private static void testMonitoringEndpointSpecs(HttpTestClient httpClient, MessageHeaders spec,
		Map<String, String> parameterMap) {
		try {
			HttpMethodWrapper method = spec.getHttpMethod();
			String path = getRestEndpointPath(spec.getTargetRestEndpointURL(), parameterMap);
			if (spec.getRequestClass() == EmptyRequestBody.class) {
				switch (method) {
					case GET:
						httpClient.sendGetRequest(path, TEST_TIMEOUT);
						break;
					case DELETE:
						httpClient.sendDeleteRequest(path, TEST_TIMEOUT);
						break;
					default:
						throw new UnsupportedOperationException("Cannot handle REST Test for " + path +
						" with method " + method + ". Only GET and DELETE requests are supported!");
				}

				HttpTestClient.SimpleHttpResponse resp = httpClient.getNextResponse();

				Preconditions.checkState(resp.getStatus().code() == spec.getResponseStatusCode().code(),
					"Found mismatching status code from endpoint " + path + " with method " + method +
					", expecting: " + spec.getResponseStatusCode().code() + ", but was: " + resp.getStatus().code());
				// System.out.println("Found matching status for endpoint " + path + " with method  " + method);
				@SuppressWarnings("unchecked")
				Object responseObject = MAPPER.readValue(resp.getContent(), spec.getResponseClass());
				Preconditions.checkNotNull(responseObject);
				testSuccessCount += 1;
			} else {
				throw new UnsupportedOperationException("Cannot handle REST Test for " + path +
					" with method " + method + ". Non-empty payload is not supported!");
			}
		} catch (IOException |
			TimeoutException |
			InterruptedException |
			IllegalStateException |
			NullPointerException e) {
			testFailureCount += 1;
			e.printStackTrace();
		} catch (UnsupportedOperationException e) {
			testSkipCount += 1;
			e.printStackTrace();
		}
	}

	/**
	 * Replace target REST endpoint with actual parameters from job launch.
	 * @param url target REST endpoint pattern from {@link MessageHeaders} specs
	 * @param parameterMap parameter replacement map
	 * @return actual REST URL literal
	 */
	private static String getRestEndpointPath(String url, Map<String, String> parameterMap)
		throws UnsupportedOperationException {
		for (Map.Entry<String, String> e : parameterMap.entrySet()) {
			if (e.getValue().equals("")) {
				if (Pattern.compile(e.getKey()).matcher(url).find()) {
					throw new UnsupportedOperationException("No parameter replacement found for " + e.getKey());
				}
			} else {
				url = url.replaceAll(e.getKey(), e.getValue());
			}
		}
		return url;
	}

	/**
	 * Utility class to extract the {@link MessageHeaders} that the {@link DispatcherRestEndpoint} supports.
	 */
	private static class E2ETestDispatcherRestEndpoint extends DispatcherRestEndpoint {

		private static final Configuration config;
		private static final RestServerEndpointConfiguration restConfig;
		private static final RestHandlerConfiguration handlerConfig;
		private static final Executor executor;
		private static final GatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;
		private static final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
		private static final MetricQueryServiceRetriever metricQueryServiceRetriever;

		static {
			config = new Configuration();
			config.setString(RestOptions.REST_ADDRESS, "localhost");
			try {
				restConfig = RestServerEndpointConfiguration.fromConfiguration(config);
			} catch (ConfigurationException e) {
				throw new RuntimeException("Implementation error. RestServerEndpointConfiguration#fromConfiguration failed for default configuration.");
			}
			handlerConfig = RestHandlerConfiguration.fromConfiguration(config);
			executor = Executors.directExecutor();

			dispatcherGatewayRetriever = () -> null;
			resourceManagerGatewayRetriever = () -> null;
			metricQueryServiceRetriever = path -> null;
		}

		private E2ETestDispatcherRestEndpoint() throws IOException {
			super(
				restConfig,
				dispatcherGatewayRetriever,
				config,
				handlerConfig,
				resourceManagerGatewayRetriever,
				NoOpTransientBlobService.INSTANCE,
				executor,
				metricQueryServiceRetriever,
				NoOpElectionService.INSTANCE,
				NoOpFatalErrorHandler.INSTANCE);
		}

		@Override
		public List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
			return super.initializeHandlers(restAddressFuture);
		}

		private enum NoOpElectionService implements LeaderElectionService {
			INSTANCE;
			@Override
			public void start(final LeaderContender contender) throws Exception {

			}

			@Override
			public void stop() throws Exception {

			}

			@Override
			public void confirmLeaderSessionID(final UUID leaderSessionID) {

			}

			@Override
			public boolean hasLeadership() {
				return false;
			}
		}

		private enum NoOpFatalErrorHandler implements FatalErrorHandler {
			INSTANCE;

			@Override
			public void onFatalError(final Throwable exception) {

			}
		}

		private enum NoOpTransientBlobService implements TransientBlobService {
			INSTANCE;

			@Override
			public File getFile(TransientBlobKey key) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public File getFile(JobID jobId, TransientBlobKey key) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public TransientBlobKey putTransient(byte[] value) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public TransientBlobKey putTransient(JobID jobId, byte[] value) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public TransientBlobKey putTransient(InputStream inputStream) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public TransientBlobKey putTransient(JobID jobId, InputStream inputStream) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean deleteFromCache(TransientBlobKey key) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean deleteFromCache(JobID jobId, TransientBlobKey key) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void close() throws IOException {}
		}

		List<MessageHeaders> getSpecs() {
			Comparator<String> comparator = new RestServerEndpoint.RestHandlerUrlComparator.CaseInsensitiveOrderComparator();
			return initializeHandlers(CompletableFuture.completedFuture(null)).stream()
				.map(tuple -> tuple.f0)
				.filter(spec -> spec instanceof MessageHeaders)
				.map(spec -> (MessageHeaders) spec)
				.sorted((spec1, spec2) -> comparator.compare(spec1.getTargetRestEndpointURL(), spec2.getTargetRestEndpointURL()))
				.collect(Collectors.toList());
		}
	}
}

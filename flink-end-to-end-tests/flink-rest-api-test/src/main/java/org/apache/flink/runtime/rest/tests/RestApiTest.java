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
import org.apache.flink.api.java.ExecutionEnvironment;
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
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.RescalingParallelismQueryParameter;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.SubtaskIndexPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointIdPathParameter;
import org.apache.flink.runtime.rest.messages.cluster.ShutdownHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptPathParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringEncoder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import scala.concurrent.duration.FiniteDuration;

/**
 * Rest API test suite.
 *
 * <p>Parameters:
 * -host Set the job manager host for completion of URL
 * -port Set the job manager port for completion of URL
 */
public class RestApiTest {
	private static final FiniteDuration TEST_TIMEOUT = new FiniteDuration(10L, TimeUnit.SECONDS);
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
	private static final Joiner JOINER = Joiner.on("\n");

	private static int testSuccessCount = 0;
	private static List<String> testSkipList = new ArrayList<>();
	private static List<String> testFailureList = new ArrayList<>();

	private static String savepointPath;

	private static Map<String, Map<String, String>> pathParameterMap = new HashMap<>();
	private static Map<String, List<String>> queryParameterMap = new HashMap<>();
	private static Map<String, String> requestBodyMap = new HashMap<>();

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		savepointPath = params.getRequired("savepointPath");
		final String host = params.get("host", "localhost");
		final int port = params.getInt("port", 8081);
		final HttpTestClient httpClient = new HttpTestClient(host, port);

		// Validate Flink cluster is running and add basic parameters.
		validateRunningFlinkJob(httpClient);

		// Get necessary parameters & request bodies for testing
		loadResource();

		// Get list of endpoints
		List<MessageHeaders> specs = new E2ETestDispatcherRestEndpoint().getSpecs();
		specs.forEach(spec -> testMonitoringEndpointSpecs(httpClient, (MessageHeaders<?, ?, ?>) spec));

		if (testFailureList.size() != 0) {
			throw new RuntimeException("There are test failures. Success: " + testSuccessCount +
				" Failures: " + testFailureList.size() + " Skipped: " + testSkipList.size() +
				"\n Failure REST APIs: \n" + JOINER.join(testFailureList) +
				"\n Skipped REST APIs: \n" + JOINER.join(testSkipList));
		} else {
			System.out.println("Test successfully completed! Success test count: " + testSuccessCount +
				"\n Skipped REST APIs: \n" + JOINER.join(testSkipList));
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.fromElements("dummy", "data").print();

		clusterShutdown(httpClient);
	}

	private static void clusterShutdown(HttpTestClient httpClient) {
		try {
			// 1. Validate Flink cluster running
			httpClient.sendDeleteRequest("/cluster", TEST_TIMEOUT);
			HttpTestClient.SimpleHttpResponse resp = httpClient.getNextResponse();
			assert resp.getStatus().code() == 200;
		} catch (Exception e) {
			throw new RuntimeException("Unable to shutdown cluster via REST API. Test Failed!");
		}
	}

	private static void validateRunningFlinkJob(HttpTestClient httpClient) {
		HttpTestClient.SimpleHttpResponse resp;
		RequestBody requestBody;
		try {
			// 1. Validate Flink cluster running
			httpClient.sendGetRequest("/jobs", TEST_TIMEOUT);
			resp = httpClient.getNextResponse();
			JobIdsWithStatusOverview jobsOverview = MAPPER.readValue(resp.getContent(),
				JobIdsWithStatusOverview.class);

			// 2. Validate task manager running in Flink cluster
			httpClient.sendGetRequest("/taskmanagers", TEST_TIMEOUT);
			resp = httpClient.getNextResponse();
			Preconditions.checkState(resp.getStatus().code() == 200,
				"Cannot fetch task manager status!");
			TaskManagersInfo taskManagersInfo = MAPPER.readValue(resp.getContent(),
				TaskManagersInfo.class);
			Optional<TaskManagerInfo> taskManagerInfo = taskManagersInfo.getTaskManagerInfos()
				.stream()
				.findFirst();
			assert taskManagerInfo.isPresent();
			pathParameterMap.put(TaskManagerIdPathParameter.KEY,
				ImmutableMap.of(":" + TaskManagerIdPathParameter.KEY,
				taskManagerInfo.get().getResourceId().toString()));

			// 3. Validate at least one running job
			Optional<JobIdsWithStatusOverview.JobIdWithStatus> job = jobsOverview
				.getJobsWithStatus()
				.stream()
				.filter(jobIdWithStatus -> jobIdWithStatus.getJobStatus() == JobStatus.RUNNING)
				.findFirst();
			assert job.isPresent();
			String jobId = job.get().getJobId().toString();
			pathParameterMap.put(JobIDPathParameter.KEY,
				ImmutableMap.of(":" + JobIDPathParameter.KEY, jobId));

			// 4. Validate job info contains all parameters
			httpClient.sendGetRequest("/jobs/" + jobId, TEST_TIMEOUT);
			resp = httpClient.getNextResponse();
			JobDetailsInfo jobsInfo = MAPPER.readValue(resp.getContent(), JobDetailsInfo.class);
			Optional<JobDetailsInfo.JobVertexDetailsInfo> vertexInfo = jobsInfo.getJobVertexInfos()
				.stream()
				.findFirst();
			assert vertexInfo.isPresent();
			pathParameterMap.put(JobVertexIdPathParameter.KEY,
				ImmutableMap.of(":" + JobVertexIdPathParameter.KEY,
				vertexInfo.get().getJobVertexID().toString()));

			// 5. Validate savepoint disposal works for the cluster
			requestBody = new SavepointDisposalRequest(savepointPath);
			httpClient.sendPostRequest("savepoint-disposal",
				Unpooled.copiedBuffer(MAPPER.writeValueAsBytes(requestBody)),
				TEST_TIMEOUT);
			resp = httpClient.getNextResponse();
			TriggerResponse savepointDisposalResponse = MAPPER.readValue(resp.getContent(), TriggerResponse.class);
			assert savepointDisposalResponse != null;

			// 6. Validate savepoint works for the job
			requestBody = new SavepointTriggerRequestBody(savepointPath, false);
			httpClient.sendPostRequest("/jobs/" + jobId + "/savepoints",
				Unpooled.copiedBuffer(MAPPER.writeValueAsBytes(requestBody)),
				TEST_TIMEOUT);
			resp = httpClient.getNextResponse();
			TriggerResponse savepointTriggerResponse = MAPPER.readValue(resp.getContent(), TriggerResponse.class);
			assert savepointTriggerResponse != null;

			// 7. Validate rescaling works for the job
			httpClient.sendPatchRequest("/jobs/" + jobId + "/rescaling?parallelism=2" , null, TEST_TIMEOUT);
			resp = httpClient.getNextResponse();
			TriggerResponse rescalingResponse = MAPPER.readValue(resp.getContent(), TriggerResponse.class);
			assert rescalingResponse != null;
			pathParameterMap.put(TriggerIdPathParameter.KEY,
				ImmutableMap.of("savepoint-disposal/:" + TriggerIdPathParameter.KEY,
					"savepoint-disposal/" + savepointDisposalResponse.getTriggerId().toString(),
					"savepoints/:" + TriggerIdPathParameter.KEY,
					"savepoints/" + savepointTriggerResponse.getTriggerId().toString(),
					"rescaling/:" + TriggerIdPathParameter.KEY,
					"rescaling/" + rescalingResponse.getTriggerId().toString()));
		} catch (Exception e) {
			throw new RuntimeException("Cannot get validate running job on Flink cluster, " +
				"please make sure cluster is running and job(s) have been submitted!", e);
		}
	}

	private static void loadResource() {

		pathParameterMap.put(CheckpointIdPathParameter.KEY,
			ImmutableMap.of(":" + CheckpointIdPathParameter.KEY, "1")); // first checkpoint
		pathParameterMap.put(SubtaskIndexPathParameter.KEY,
			ImmutableMap.of(":" + SubtaskIndexPathParameter.KEY, "0")); // first subtask
		pathParameterMap.put(SubtaskAttemptPathParameter.KEY,
			ImmutableMap.of(":" + SubtaskAttemptPathParameter.KEY, "0")); // first attempt
		queryParameterMap.put(RescalingParallelismQueryParameter.KEY, ImmutableList.of("4"));

		// Load RequestBody payloads
		try (InputStream in = RestApiTest.class.getClassLoader().getResourceAsStream("requestBodies.yaml")) {
			JsonNode jsonNode = YAML_MAPPER.readTree(in).get("requestBodies");
			for (final JsonNode requestBodyNode : jsonNode) {
				requestBodyMap.put(requestBodyNode.get("url").asText(),
					resolvePayloadParameter(requestBodyNode.get("payload").asText()));
			}
		} catch (Exception e) {
			throw new RuntimeException("Cannot load necessary resources for e2e REST API test!", e);
		}
	}

	private static String resolvePayloadParameter(String payload) {
		return payload
			.replace("${savepointPath}", savepointPath);
	}

	private static <R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
		void testMonitoringEndpointSpecs(HttpTestClient httpClient, MessageHeaders<R, P, M> spec) {
		System.out.println("************************************************");
		try {
			String targetUrl = spec.getTargetRestEndpointURL();
			System.out.println("Target URL: " + targetUrl + " Method: " + spec.getHttpMethod());
			String url = resolveParamsForUrl(targetUrl, spec);
			System.out.println("Resolved URL: " + url);
			RequestBody requestBody;
			switch (spec.getHttpMethod()) {
				case GET:
					httpClient.sendGetRequest(url, TEST_TIMEOUT);
					break;
				case DELETE:
					httpClient.sendDeleteRequest(url, TEST_TIMEOUT);
					break;
				case PATCH:
					if (spec.getRequestClass() != EmptyRequestBody.class) {
						requestBody = resolveRequestBody(spec);
					} else {
						requestBody = null;
					}
					httpClient.sendPatchRequest(url,
						Unpooled.copiedBuffer(MAPPER.writeValueAsBytes(requestBody)),
						TEST_TIMEOUT);
					break;
				case POST:
					if (spec.getRequestClass() != EmptyRequestBody.class) {
						requestBody = resolveRequestBody(spec);
					} else {
						requestBody = null;
					}
					httpClient.sendPostRequest(url,
						Unpooled.copiedBuffer(MAPPER.writeValueAsBytes(requestBody)),
						TEST_TIMEOUT);
					break;
				default:
					throw new UnsupportedOperationException("Unknown REST type: " + spec.getHttpMethod());
			}
			HttpTestClient.SimpleHttpResponse resp = httpClient.getNextResponse();
			System.out.println("Return Code: " + resp.getStatus().code());
			assert resp.getStatus().code() == spec.getResponseStatusCode().code();

			Object responseObject = MAPPER.readValue(resp.getContent(), spec.getResponseClass());
			System.out.println("Return Object: " + responseObject.toString());
			assert responseObject.getClass() == spec.getResponseClass();
			testSuccessCount += 1;
			System.out.println("================================================");
		} catch (UnsupportedOperationException e) {
			testSkipList.add(
				"URL: " + spec.getTargetRestEndpointURL() + "\n"
				+ "METHOD: " + spec.getHttpMethod() + "\n"
			);
		} catch (Exception e) {
			testFailureList.add(
				"URL: " + spec.getTargetRestEndpointURL() + "\n"
				+ "METHOD: " + spec.getHttpMethod() + "\n"
			);
		}
	}

	private static String resolveParamsForUrl(String targetUrl, MessageHeaders specs)
		throws UnsupportedOperationException {
		MessageParameters unresolvedMessageParameters = specs.getUnresolvedMessageParameters();
		try {
			for (MessagePathParameter pathParam : unresolvedMessageParameters.getPathParameters()) {
				Map<String, String> paramValues = pathParameterMap.get(pathParam.getKey());
				if (pathParam.isMandatory()) {
					Preconditions.checkNotNull(paramValues,
						"Cannot find path parameter for: " + pathParam.getKey());
				}
				if (paramValues != null) {
					for (Map.Entry<String, String> paramValue : paramValues.entrySet()) {
						targetUrl = targetUrl.replace(paramValue.getKey(), paramValue.getValue());
					}
				}
			}
			QueryStringEncoder queryStringEncoder = new QueryStringEncoder(targetUrl);
			for (MessageQueryParameter queryParam : unresolvedMessageParameters.getQueryParameters()) {
				List<String> paramValue = queryParameterMap.get(queryParam.getKey());
				if (queryParam.isMandatory()) {
					Preconditions.checkNotNull(paramValue,
						"Cannot find query parameter for: " + queryParam.getKey());
				}
				if (paramValue != null) {
					queryStringEncoder.addParam(queryParam.getKey(), Joiner.on(",").join(paramValue));
				}
			}
			return queryStringEncoder.toString();
		} catch (Exception e) {
			throw new UnsupportedOperationException(e);
		}
	}

	private static <R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
		RequestBody resolveRequestBody(MessageHeaders<R, P, M> spec) {
		RequestBody requestBody = null;
		try {
			requestBody = MAPPER.readValue(
				requestBodyMap.get(spec.getTargetRestEndpointURL()),
				spec.getRequestClass()
			);
			if (spec.getRequestClass() != EmptyRequestBody.class && requestBody == null) {
				throw new UnsupportedOperationException(
					"Can't find request body for endpoint: [" + spec.getTargetRestEndpointURL() + "]");
			}
		} catch (Exception e) {
			throw new UnsupportedOperationException(
				"Can't convert request body for endpoint: [" + spec.getTargetRestEndpointURL() +
				"] Expect serialized payload of Class: " + spec.getRequestClass().getName()
			);
		}
		return requestBody;
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
			config.setString(RestOptions.ADDRESS, "localhost");
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
				.filter(tuple -> tuple.f0 instanceof MessageHeaders)
				.filter(tuple -> !(tuple.f0 instanceof ShutdownHeaders)) // we cannot run shutdown along the path
				.map(tuple -> (MessageHeaders) tuple.f0)
				.sorted((spec1, spec2) -> comparator.compare(
					spec1.getTargetRestEndpointURL() + spec1.getHttpMethod(),
					spec2.getTargetRestEndpointURL() + spec2.getHttpMethod()))
				.collect(Collectors.toList());
		}
	}
}

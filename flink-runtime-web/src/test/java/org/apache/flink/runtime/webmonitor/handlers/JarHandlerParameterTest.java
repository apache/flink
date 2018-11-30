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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/** Base test class for jar request handlers. */
public abstract class JarHandlerParameterTest
	<REQB extends JarRequestBody, M extends JarMessageParameters> extends TestLogger {
	enum ProgramArgsParType {
		String,
		List,
		Both
	}

	static final String[] PROG_ARGS = new String[] {"--host", "localhost", "--port", "1234"};
	static final int PARALLELISM = 4;

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	@ClassRule
	public static final BlobServerResource BLOB_SERVER_RESOURCE = new BlobServerResource();

	static final AtomicReference<JobGraph> LAST_SUBMITTED_JOB_GRAPH_REFERENCE = new AtomicReference<>();

	static TestingDispatcherGateway restfulGateway;
	static Path jarDir;
	static GatewayRetriever<TestingDispatcherGateway> gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
	static CompletableFuture<String> localAddressFuture = CompletableFuture.completedFuture("shazam://localhost:12345");
	static Time timeout = Time.seconds(10);
	static Map<String, String> responseHeaders = Collections.emptyMap();
	static Executor executor = TestingUtils.defaultExecutor();

	private static Path jarWithManifest;
	private static Path jarWithoutManifest;

	static void init() throws Exception {
		jarDir = TMP.newFolder().toPath();

		// properties are set property by surefire plugin
		final String parameterProgramJarName = System.getProperty("parameterJarName") + ".jar";
		final String parameterProgramWithoutManifestJarName = System.getProperty("parameterJarWithoutManifestName") + ".jar";
		final Path jarLocation = Paths.get(System.getProperty("targetDir"));

		jarWithManifest = Files.copy(
			jarLocation.resolve(parameterProgramJarName),
			jarDir.resolve("program-with-manifest.jar"));
		jarWithoutManifest = Files.copy(
			jarLocation.resolve(parameterProgramWithoutManifestJarName),
			jarDir.resolve("program-without-manifest.jar"));

		restfulGateway = new TestingDispatcherGateway.Builder()
			.setBlobServerPort(BLOB_SERVER_RESOURCE.getBlobServerPort())
			.setSubmitFunction(jobGraph -> {
				LAST_SUBMITTED_JOB_GRAPH_REFERENCE.set(jobGraph);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.build();

		gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
		localAddressFuture = CompletableFuture.completedFuture("shazam://localhost:12345");
		timeout = Time.seconds(10);
		responseHeaders = Collections.emptyMap();
		executor = TestingUtils.defaultExecutor();
	}

	@Before
	public void reset() {
		ParameterProgram.actualArguments = null;
	}

	@Test
	public void testDefaultParameters() throws Exception {
		// baseline, ensure that reasonable defaults are chosen
		handleRequest(createRequest(
			getDefaultJarRequestBody(),
			getUnresolvedJarMessageParameters(),
			getUnresolvedJarMessageParameters(),
			jarWithManifest));
		validateDefaultGraph();
	}

	@Test
	public void testConfigurationViaQueryParametersWithProgArgsAsString() throws Exception {
		testConfigurationViaQueryParameters(ProgramArgsParType.String);
	}

	@Test
	public void testConfigurationViaQueryParametersWithProgArgsAsList() throws Exception {
		testConfigurationViaQueryParameters(ProgramArgsParType.List);
	}

	@Test
	public void testConfigurationViaQueryParametersFailWithProgArgsAsStringAndList() throws Exception {
		try {
			testConfigurationViaQueryParameters(ProgramArgsParType.Both);
			fail("RestHandlerException is excepted");
		} catch (RestHandlerException e) {
			assertEquals(HttpResponseStatus.BAD_REQUEST, e.getHttpResponseStatus());
		}
	}

	private void testConfigurationViaQueryParameters(ProgramArgsParType programArgsParType) throws Exception {
		// configure submission via query parameters
		handleRequest(createRequest(
			getDefaultJarRequestBody(),
			getJarMessageParameters(programArgsParType),
			getUnresolvedJarMessageParameters(),
			jarWithoutManifest));
		validateGraph();
	}

	@Test
	public void testConfigurationViaJsonRequestWithProgArgsAsString() throws Exception {
		testConfigurationViaJsonRequest(ProgramArgsParType.String);
	}

	@Test
	public void testConfigurationViaJsonRequestWithProgArgsAsList() throws Exception {
		testConfigurationViaJsonRequest(ProgramArgsParType.List);
	}

	@Test
	public void testConfigurationViaJsonRequestFailWithProgArgsAsStringAndList() throws Exception {
		try {
			testConfigurationViaJsonRequest(ProgramArgsParType.Both);
			fail("RestHandlerException is excepted");
		} catch (RestHandlerException e) {
			assertEquals(HttpResponseStatus.BAD_REQUEST, e.getHttpResponseStatus());
		}
	}

	private void testConfigurationViaJsonRequest(ProgramArgsParType programArgsParType) throws Exception {
		handleRequest(createRequest(
			getJarRequestBody(programArgsParType),
			getUnresolvedJarMessageParameters(),
			getUnresolvedJarMessageParameters(),
			jarWithoutManifest
		));
		validateGraph();
	}

	@Test
	public void testParameterPrioritizationWithProgArgsAsString() throws Exception {
		testParameterPrioritization(ProgramArgsParType.String);
	}

	@Test
	public void testParameterPrioritizationWithProgArgsAsList() throws Exception {
		testParameterPrioritization(ProgramArgsParType.List);
	}

	@Test
	public void testFailIfProgArgsAreAsStringAndAsList() throws Exception {
		try {
			testParameterPrioritization(ProgramArgsParType.Both);
			fail("RestHandlerException is excepted");
		} catch (RestHandlerException e) {
			assertEquals(HttpResponseStatus.BAD_REQUEST, e.getHttpResponseStatus());
		}
	}

	private void testParameterPrioritization(ProgramArgsParType programArgsParType) throws Exception {
		// configure submission via query parameters and JSON request, JSON should be prioritized
		handleRequest(createRequest(
			getJarRequestBody(programArgsParType),
			getWrongJarMessageParameters(programArgsParType),
			getUnresolvedJarMessageParameters(),
			jarWithoutManifest));
		validateGraph();
	}

	static String getProgramArgsString(ProgramArgsParType programArgsParType) {
		return programArgsParType == ProgramArgsParType.String || programArgsParType == ProgramArgsParType.Both
			? String.join(" ", PROG_ARGS) : null;
	}

	static List<String> getProgramArgsList(ProgramArgsParType programArgsParType) {
		return programArgsParType == ProgramArgsParType.List || programArgsParType == ProgramArgsParType.Both
			? Arrays.asList(PROG_ARGS) : null;
	}

	private static <REQB extends JarRequestBody, M extends JarMessageParameters>
	HandlerRequest<REQB, M> createRequest(
		REQB requestBody, M parameters, M unresolvedMessageParameters, Path jar)
		throws HandlerRequestException {

		final Map<String, List<String>> queryParameterAsMap = parameters.getQueryParameters().stream()
			.filter(MessageParameter::isResolved)
			.collect(Collectors.toMap(
				MessageParameter::getKey,
				JarHandlerParameterTest::getValuesAsString
			));

		return new HandlerRequest<>(
			requestBody,
			unresolvedMessageParameters,
			Collections.singletonMap(JarIdPathParameter.KEY, jar.getFileName().toString()),
			queryParameterAsMap,
			Collections.emptyList()
		);
	}

	private static <X> List<String> getValuesAsString(MessageQueryParameter<X> parameter) {
		final List<X> values = parameter.getValue();
		return values.stream().map(parameter::convertValueToString).collect(Collectors.toList());
	}

	abstract M getUnresolvedJarMessageParameters();

	abstract M getJarMessageParameters(ProgramArgsParType programArgsParType);

	abstract M getWrongJarMessageParameters(ProgramArgsParType programArgsParType);

	abstract REQB getDefaultJarRequestBody();

	abstract REQB getJarRequestBody(ProgramArgsParType programArgsParType);

	abstract void handleRequest(HandlerRequest<REQB, M> request) throws Exception;

	JobGraph validateDefaultGraph() {
		JobGraph jobGraph = LAST_SUBMITTED_JOB_GRAPH_REFERENCE.getAndSet(null);
		Assert.assertEquals(0, ParameterProgram.actualArguments.length);
		Assert.assertEquals(ExecutionConfig.PARALLELISM_DEFAULT, getExecutionConfig(jobGraph).getParallelism());
		return jobGraph;
	}

	JobGraph validateGraph() {
		JobGraph jobGraph = LAST_SUBMITTED_JOB_GRAPH_REFERENCE.getAndSet(null);
		Assert.assertArrayEquals(PROG_ARGS, ParameterProgram.actualArguments);
		Assert.assertEquals(PARALLELISM, getExecutionConfig(jobGraph).getParallelism());
		return jobGraph;
	}

	private static ExecutionConfig getExecutionConfig(JobGraph jobGraph) {
		ExecutionConfig executionConfig;
		try {
			executionConfig = jobGraph.getSerializedExecutionConfig().deserializeValue(ParameterProgram.class.getClassLoader());
		} catch (Exception e) {
			throw new AssertionError("Exception while deserializing ExecutionConfig.", e);
		}
		return executionConfig;
	}
}

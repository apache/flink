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
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Tests for the parameter handling of the {@link JarRunHandler}.
 */
public class JarRunHandlerParameterTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	@ClassRule
	public static final BlobServerResource BLOB_SERVER_RESOURCE = new BlobServerResource();

	private static final AtomicReference<JobGraph> lastSubmittedJobGraphReference = new AtomicReference<>();
	private static JarRunHandler handler;
	private static Path jarWithManifest;
	private static Path jarWithoutManifest;
	private static TestingDispatcherGateway restfulGateway;

	@BeforeClass
	public static void setup() throws Exception {
		Path jarDir = TMP.newFolder().toPath();

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

		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			TMP.newFolder().getAbsolutePath());

		restfulGateway = new TestingDispatcherGateway.Builder()
			.setBlobServerPort(BLOB_SERVER_RESOURCE.getBlobServerPort())
			.setSubmitFunction(jobGraph -> {
				lastSubmittedJobGraphReference.set(jobGraph);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.build();
		final GatewayRetriever<TestingDispatcherGateway> gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
		final CompletableFuture<String> localAddressFuture = CompletableFuture.completedFuture("shazam://localhost:12345");
		final Time timeout = Time.seconds(10);
		final Map<String, String> responseHeaders = Collections.emptyMap();
		final Executor executor = TestingUtils.defaultExecutor();

		handler = new JarRunHandler(
			localAddressFuture,
			gatewayRetriever,
			timeout,
			responseHeaders,
			JarRunHeaders.getInstance(),
			jarDir,
			new Configuration(),
			executor);
	}

	@Before
	public void reset() {
		ParameterProgram.actualArguments = null;
	}

	@Test
	public void testDefaultParameters() throws Exception {
		// baseline, ensure that reasonable defaults are chosen
		sendRequestAndValidateGraph(
			handler,
			restfulGateway,
			() -> createRequest(
				new JarRunRequestBody(),
				JarRunHeaders.getInstance().getUnresolvedMessageParameters(),
				jarWithManifest
			),
			jobGraph -> {
				Assert.assertEquals(0, ParameterProgram.actualArguments.length);

				Assert.assertEquals(ExecutionConfig.PARALLELISM_DEFAULT, getExecutionConfig(jobGraph).getParallelism());

				final SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
				Assert.assertFalse(savepointRestoreSettings.allowNonRestoredState());
				Assert.assertNull(savepointRestoreSettings.getRestorePath());
			}
		);
	}

	@Test
	public void testConfigurationViaQueryParameters() throws Exception {
		// configure submission via query parameters
		sendRequestAndValidateGraph(
			handler,
			restfulGateway,
			() -> {
				final JarRunMessageParameters parameters = JarRunHeaders.getInstance().getUnresolvedMessageParameters();
				parameters.allowNonRestoredStateQueryParameter.resolve(Collections.singletonList(true));
				parameters.savepointPathQueryParameter.resolve(Collections.singletonList("/foo/bar"));
				parameters.entryClassQueryParameter.resolve(Collections.singletonList(ParameterProgram.class.getCanonicalName()));
				parameters.parallelismQueryParameter.resolve(Collections.singletonList(4));
				parameters.programArgsQueryParameter.resolve(Collections.singletonList("--host localhost --port 1234"));

				return createRequest(
					new JarRunRequestBody(),
					parameters,
					jarWithoutManifest
				);
			},
			jobGraph -> {
				Assert.assertEquals(4, ParameterProgram.actualArguments.length);
				Assert.assertEquals("--host", ParameterProgram.actualArguments[0]);
				Assert.assertEquals("localhost", ParameterProgram.actualArguments[1]);
				Assert.assertEquals("--port", ParameterProgram.actualArguments[2]);
				Assert.assertEquals("1234", ParameterProgram.actualArguments[3]);

				Assert.assertEquals(4, getExecutionConfig(jobGraph).getParallelism());

				final SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
				Assert.assertTrue(savepointRestoreSettings.allowNonRestoredState());
				Assert.assertEquals("/foo/bar", savepointRestoreSettings.getRestorePath());
			}
		);
	}

	@Test
	public void testConfigurationViaJsonRequest() throws Exception {
		sendRequestAndValidateGraph(
			handler,
			restfulGateway,
			() -> {
				final JarRunRequestBody jsonRequest = new JarRunRequestBody(
					ParameterProgram.class.getCanonicalName(),
					"--host localhost --port 1234",
					4,
					true,
					"/foo/bar"
				);

				return createRequest(
					jsonRequest,
					JarRunHeaders.getInstance().getUnresolvedMessageParameters(),
					jarWithoutManifest
				);
			},
			jobGraph -> {
				Assert.assertEquals(4, ParameterProgram.actualArguments.length);
				Assert.assertEquals("--host", ParameterProgram.actualArguments[0]);
				Assert.assertEquals("localhost", ParameterProgram.actualArguments[1]);
				Assert.assertEquals("--port", ParameterProgram.actualArguments[2]);
				Assert.assertEquals("1234", ParameterProgram.actualArguments[3]);

				Assert.assertEquals(4, getExecutionConfig(jobGraph).getParallelism());

				final SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
				Assert.assertTrue(savepointRestoreSettings.allowNonRestoredState());
				Assert.assertEquals("/foo/bar", savepointRestoreSettings.getRestorePath());
			}
		);
	}

	@Test
	public void testParameterPrioritization() throws Exception {
		// configure submission via query parameters and JSON request, JSON should be prioritized
		sendRequestAndValidateGraph(
			handler,
			restfulGateway,
			() -> {
				final JarRunRequestBody jsonRequest = new JarRunRequestBody(
					ParameterProgram.class.getCanonicalName(),
					"--host localhost --port 1234",
					4,
					true,
					"/foo/bar"
				);

				final JarRunMessageParameters parameters = JarRunHeaders.getInstance().getUnresolvedMessageParameters();
				parameters.allowNonRestoredStateQueryParameter.resolve(Collections.singletonList(false));
				parameters.savepointPathQueryParameter.resolve(Collections.singletonList("/no/uh"));
				parameters.entryClassQueryParameter.resolve(Collections.singletonList("please.dont.run.me"));
				parameters.parallelismQueryParameter.resolve(Collections.singletonList(64));
				parameters.programArgsQueryParameter.resolve(Collections.singletonList("--host wrong --port wrong"));

				return createRequest(
					jsonRequest,
					parameters,
					jarWithoutManifest
				);
			},
			jobGraph -> {
				Assert.assertEquals(4, ParameterProgram.actualArguments.length);
				Assert.assertEquals("--host", ParameterProgram.actualArguments[0]);
				Assert.assertEquals("localhost", ParameterProgram.actualArguments[1]);
				Assert.assertEquals("--port", ParameterProgram.actualArguments[2]);
				Assert.assertEquals("1234", ParameterProgram.actualArguments[3]);

				Assert.assertEquals(4, getExecutionConfig(jobGraph).getParallelism());

				final SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
				Assert.assertTrue(savepointRestoreSettings.allowNonRestoredState());
				Assert.assertEquals("/foo/bar", savepointRestoreSettings.getRestorePath());
			}
		);
	}

	private static HandlerRequest<JarRunRequestBody, JarRunMessageParameters> createRequest(
			JarRunRequestBody requestBody,
			JarRunMessageParameters parameters,
			Path jar) throws HandlerRequestException {

		final Map<String, List<String>> queryParameterAsMap = parameters.getQueryParameters().stream()
			.filter(MessageParameter::isResolved)
			.collect(Collectors.toMap(
				MessageParameter::getKey,
				JarRunHandlerParameterTest::getValuesAsString
			));

		return new HandlerRequest<>(
			requestBody,
			JarRunHeaders.getInstance().getUnresolvedMessageParameters(),
			Collections.singletonMap(JarIdPathParameter.KEY, jar.getFileName().toString()),
			queryParameterAsMap,
			Collections.emptyList()
		);
	}

	private static void sendRequestAndValidateGraph(
			JarRunHandler handler,
			DispatcherGateway dispatcherGateway,
			SupplierWithException<HandlerRequest<JarRunRequestBody, JarRunMessageParameters>, HandlerRequestException> requestSupplier,
			ThrowingConsumer<JobGraph, AssertionError> validator) throws Exception {

		handler.handleRequest(requestSupplier.get(), dispatcherGateway)
			.get();

		JobGraph submittedJobGraph = lastSubmittedJobGraphReference.getAndSet(null);

		validator.accept(submittedJobGraph);
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

	private static <X> List<String> getValuesAsString(MessageQueryParameter<X> parameter) {
		final List<X> values = parameter.getValue();
		return values.stream().map(parameter::convertValueToString).collect(Collectors.toList());
	}
}

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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests the entire lifecycle of an artifact submission.
 */
public class ArtifactSubmissionITCase extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final BlobServerResource blobServerResource = new BlobServerResource();

	@BeforeClass
	public static void checkOS() {
		Assume.assumeFalse("This test fails on Windows due to unclosed JarFiles, see FLINK-9844.", OperatingSystem.isWindows());
	}

	@Test
	public void testJarSubmission() throws Exception {
		final TestingDispatcherGateway restfulGateway = new TestingDispatcherGateway.Builder()
			.setBlobServerPort(blobServerResource.getBlobServerPort())
			.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
			.build();
		final JarHandlers handlers = new JarHandlers(temporaryFolder.newFolder().toPath(), restfulGateway);
		final ArtifactUploadHandler uploadHandler = handlers.uploadHandler;
		final ArtifactListHandler listHandler = handlers.listHandler;
		final ArtifactPlanHandler planHandler = handlers.planHandler;
		final ArtifactRunHandler runHandler = handlers.runHandler;
		final ArtifactDeleteHandler deleteHandler = handlers.deleteHandler;

		// targetDir property is set via surefire configuration
		final Path originalJar = Paths.get(System.getProperty("targetDir")).resolve("test-program.jar");
		final Path jar = Files.copy(originalJar, temporaryFolder.getRoot().toPath().resolve("test-program.jar"));

		final String storedJarPath = uploadJar(uploadHandler, jar, restfulGateway);
		final String storedJarName = Paths.get(storedJarPath).getFileName().toString();

		final ArtifactListInfo postUploadListResponse = listJars(listHandler, restfulGateway);
		Assert.assertEquals(1, postUploadListResponse.artifactFileList.size());
		final ArtifactListInfo.ArtifactFileInfo listEntry = postUploadListResponse.artifactFileList.iterator().next();
		Assert.assertEquals(jar.getFileName().toString(), listEntry.name);
		Assert.assertEquals(storedJarName, listEntry.id);

		final JobPlanInfo planResponse = showPlan(planHandler, storedJarName, restfulGateway);
		// we're only interested in the core functionality so checking for a small detail is sufficient
		Assert.assertThat(planResponse.getJsonPlan(), containsString("TestProgram.java:29"));

		runJar(runHandler, storedJarName, restfulGateway);

		deleteJar(deleteHandler, storedJarName, restfulGateway);

		final ArtifactListInfo postDeleteListResponse = listJars(listHandler, restfulGateway);
		Assert.assertEquals(0, postDeleteListResponse.artifactFileList.size());
	}

	private static String uploadJar(ArtifactUploadHandler handler, Path jar, RestfulGateway restfulGateway) throws Exception {
		HandlerRequest<EmptyRequestBody, EmptyMessageParameters> uploadRequest = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			EmptyMessageParameters.getInstance(),
			Collections.emptyMap(),
			Collections.emptyMap(),
			Collections.singletonList(jar.toFile()));
		final ArtifactUploadResponseBody uploadResponse = handler.handleRequest(uploadRequest, restfulGateway)
			.get();
		return uploadResponse.getFilename();
	}

	private static ArtifactListInfo listJars(ArtifactListHandler handler, RestfulGateway restfulGateway) throws Exception {
		HandlerRequest<EmptyRequestBody, EmptyMessageParameters> listRequest = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			EmptyMessageParameters.getInstance());
		return handler.handleRequest(listRequest, restfulGateway)
			.get();
	}

	private static JobPlanInfo showPlan(ArtifactPlanHandler handler, String jarName, RestfulGateway restfulGateway) throws Exception {
		ArtifactPlanMessageParameters planParameters = ArtifactPlanGetHeaders.getInstance().getUnresolvedMessageParameters();
		HandlerRequest<ArtifactPlanRequestBody, ArtifactPlanMessageParameters> planRequest = new HandlerRequest<>(
			new ArtifactPlanRequestBody(),
			planParameters,
			Collections.singletonMap(planParameters.artifactIdPathParameter.getKey(), jarName),
			Collections.emptyMap(),
			Collections.emptyList());
		return handler.handleRequest(planRequest, restfulGateway)
			.get();
	}

	private static ArtifactRunResponseBody runJar(ArtifactRunHandler handler, String jarName, DispatcherGateway restfulGateway) throws Exception {
		final ArtifactRunMessageParameters runParameters = ArtifactRunHeaders.getInstance().getUnresolvedMessageParameters();
		HandlerRequest<ArtifactRunRequestBody, ArtifactRunMessageParameters> runRequest = new HandlerRequest<>(
			new ArtifactRunRequestBody(),
			runParameters,
			Collections.singletonMap(runParameters.artifactIdPathParameter.getKey(), jarName),
			Collections.emptyMap(),
			Collections.emptyList());
		return handler.handleRequest(runRequest, restfulGateway)
			.get();
	}

	private static void deleteJar(ArtifactDeleteHandler handler, String jarName, RestfulGateway restfulGateway) throws Exception {
		ArtifactDeleteMessageParameters deleteParameters = ArtifactDeleteHeaders.getInstance().getUnresolvedMessageParameters();
		HandlerRequest<EmptyRequestBody, ArtifactDeleteMessageParameters> deleteRequest = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			deleteParameters,
			Collections.singletonMap(deleteParameters.artifactIdPathParameter.getKey(), jarName),
			Collections.emptyMap(),
			Collections.emptyList());
		handler.handleRequest(deleteRequest, restfulGateway)
			.get();
	}

	private static class JarHandlers {
		final ArtifactUploadHandler uploadHandler;
		final ArtifactListHandler listHandler;
		final ArtifactPlanHandler planHandler;
		final ArtifactRunHandler runHandler;
		final ArtifactDeleteHandler deleteHandler;

		JarHandlers(final Path jarDir, final TestingDispatcherGateway restfulGateway) {
			final GatewayRetriever<TestingDispatcherGateway> gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
			final Time timeout = Time.seconds(10);
			final Map<String, String> responseHeaders = Collections.emptyMap();
			final Executor executor = TestingUtils.defaultExecutor();

			uploadHandler = new ArtifactUploadHandler(
				gatewayRetriever,
				timeout,
				responseHeaders,
				ArtifactUploadHeaders.getInstance(),
				jarDir,
				executor);

			listHandler = new ArtifactListHandler(
				gatewayRetriever,
				timeout,
				responseHeaders,
				ArtifactListHeaders.getInstance(),
				CompletableFuture.completedFuture("shazam://localhost:12345"),
				jarDir.toFile(),
				executor);

			planHandler = new ArtifactPlanHandler(
				gatewayRetriever,
				timeout,
				responseHeaders,
				ArtifactPlanGetHeaders.getInstance(),
				jarDir,
				new Configuration(),
				executor);

			runHandler = new ArtifactRunHandler(
				gatewayRetriever,
				timeout,
				responseHeaders,
				ArtifactRunHeaders.getInstance(),
				jarDir,
				new Configuration(),
				executor);

			deleteHandler = new ArtifactDeleteHandler(
				gatewayRetriever,
				timeout,
				responseHeaders,
				ArtifactDeleteHeaders.getInstance(),
				jarDir,
				executor);
		}
	}
}

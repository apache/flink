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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.net.SSLUtilsTest;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Tests for the {@link JobSubmitHandler}.
 */
@RunWith(Parameterized.class)
public class JobSubmitHandlerTest extends TestLogger {

	@Parameterized.Parameters(name = "SSL enabled: {0}")
	public static Iterable<Boolean> data() {
		return Arrays.asList(true, false);
	}

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private final Configuration configuration;

	private BlobServer blobServer;

	public JobSubmitHandlerTest(boolean withSsl) {
		this.configuration = withSsl
			? SSLUtilsTest.createInternalSslConfigWithKeyAndTrustStores()
			: new Configuration();
	}

	@Before
	public void setup() throws IOException {
		Configuration config = new Configuration(configuration);
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			TEMPORARY_FOLDER.newFolder().getAbsolutePath());

		blobServer = new BlobServer(config, new VoidBlobStore());
		blobServer.start();
	}

	@After
	public void teardown() throws IOException {
		if (blobServer != null) {
			blobServer.close();
		}
	}

	@Test
	public void testSerializationFailureHandling() throws Exception {
		final Path jobGraphFile = TEMPORARY_FOLDER.newFile().toPath();
		DispatcherGateway mockGateway = new TestingDispatcherGateway.Builder()
			.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
			.build();

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			() -> CompletableFuture.completedFuture(mockGateway),
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			TestingUtils.defaultExecutor(),
			configuration);

		JobSubmitRequestBody request = new JobSubmitRequestBody(jobGraphFile.toString(), Collections.emptyList());

		try {
			handler.handleRequest(new HandlerRequest<>(request, EmptyMessageParameters.getInstance()), mockGateway);
			Assert.fail();
		} catch (RestHandlerException rhe) {
			Assert.assertEquals(HttpResponseStatus.BAD_REQUEST, rhe.getHttpResponseStatus());
		}
	}

	@Test
	public void testSuccessfulJobSubmission() throws Exception {
		final Path jobGraphFile = TEMPORARY_FOLDER.newFile().toPath();
		try (ObjectOutputStream objectOut = new ObjectOutputStream(Files.newOutputStream(jobGraphFile))) {
			objectOut.writeObject(new JobGraph("testjob"));
		}

		TestingDispatcherGateway.Builder builder = new TestingDispatcherGateway.Builder();
		builder
			.setBlobServerPort(blobServer.getPort())
			.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
			.setHostname("localhost");
		DispatcherGateway mockGateway = builder.build();

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			() -> CompletableFuture.completedFuture(mockGateway),
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			TestingUtils.defaultExecutor(),
			configuration);

		JobSubmitRequestBody request = new JobSubmitRequestBody(jobGraphFile.getFileName().toString(), Collections.emptyList());

		handler.handleRequest(new HandlerRequest<>(request, EmptyMessageParameters.getInstance(), Collections.emptyMap(), Collections.emptyMap(), Collections.singleton(jobGraphFile.toFile())), mockGateway)
			.get();
	}

	@Test
	public void testFailedJobSubmission() throws Exception {
		final String errorMessage = "test";
		DispatcherGateway mockGateway = new TestingDispatcherGateway.Builder()
			.setSubmitFunction(jobgraph -> FutureUtils.completedExceptionally(new Exception(errorMessage)))
			.build();

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			() -> CompletableFuture.completedFuture(mockGateway),
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			TestingUtils.defaultExecutor(),
			configuration);

		final Path jobGraphFile = TEMPORARY_FOLDER.newFile().toPath();

		JobGraph jobGraph = new JobGraph("testjob");
		try (ObjectOutputStream objectOut = new ObjectOutputStream(Files.newOutputStream(jobGraphFile))) {
			objectOut.writeObject(jobGraph);
		}
		JobSubmitRequestBody request = new JobSubmitRequestBody(jobGraphFile.getFileName().toString(), Collections.emptyList());

		try {
			handler.handleRequest(new HandlerRequest<>(
					request,
					EmptyMessageParameters.getInstance(),
					Collections.emptyMap(),
					Collections.emptyMap(),
					Collections.singletonList(jobGraphFile.toFile())), mockGateway)
				.get();
		} catch (Exception e) {
			Throwable t = ExceptionUtils.stripExecutionException(e);
			Assert.assertEquals(errorMessage, t.getMessage());
		}
	}

	@Test
	public void testRejectionOnCountMismatch() throws Exception {
		final Path jobGraphFile = TEMPORARY_FOLDER.newFile().toPath();
		try (ObjectOutputStream objectOut = new ObjectOutputStream(Files.newOutputStream(jobGraphFile))) {
			objectOut.writeObject(new JobGraph("testjob"));
		}
		final Path countExceedingFile = TEMPORARY_FOLDER.newFile().toPath();

		TestingDispatcherGateway.Builder builder = new TestingDispatcherGateway.Builder();
		builder
			.setBlobServerPort(blobServer.getPort())
			.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
			.setHostname("localhost");
		DispatcherGateway mockGateway = builder.build();

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			() -> CompletableFuture.completedFuture(mockGateway),
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			TestingUtils.defaultExecutor(),
			configuration);

		JobSubmitRequestBody request = new JobSubmitRequestBody(jobGraphFile.getFileName().toString(), Collections.emptyList());

		try {
			handler.handleRequest(new HandlerRequest<>(request, EmptyMessageParameters.getInstance(), Collections.emptyMap(), Collections.emptyMap(), Arrays.asList(jobGraphFile.toFile(), countExceedingFile.toFile())), mockGateway)
				.get();
		} catch (Exception e) {
			ExceptionUtils.findThrowable(e, candidate -> candidate instanceof RestHandlerException && candidate.getMessage().contains("count"));
		}
	}

	@Test
	public void testFileHandling() throws Exception {
		CompletableFuture<JobGraph> submittedJobGraphFuture = new CompletableFuture<>();
		DispatcherGateway dispatcherGateway = new TestingDispatcherGateway.Builder()
			.setBlobServerPort(blobServer.getPort())
			.setSubmitFunction(submittedJobGraph -> {
				submittedJobGraphFuture.complete(submittedJobGraph);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.build();

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			() -> CompletableFuture.completedFuture(dispatcherGateway),
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			TestingUtils.defaultExecutor(),
			configuration);

		final Path jobGraphFile = TEMPORARY_FOLDER.newFile().toPath();
		final Path jarFile = TEMPORARY_FOLDER.newFile().toPath();
		final Path artifactFile = TEMPORARY_FOLDER.newFile().toPath();

		final JobGraph jobGraph = new JobGraph();
		try (ObjectOutputStream objectOut = new ObjectOutputStream(Files.newOutputStream(jobGraphFile))) {
			objectOut.writeObject(jobGraph);
		}

		JobSubmitRequestBody request = new JobSubmitRequestBody(
			jobGraphFile.getFileName().toString(),
			Collections.singletonList(jarFile.getFileName().toString()));

		handler.handleRequest(new HandlerRequest<>(
				request,
				EmptyMessageParameters.getInstance(),
				Collections.emptyMap(),
				Collections.emptyMap(),
				Arrays.asList(jobGraphFile.toFile(), jarFile.toFile(), artifactFile.toFile())), dispatcherGateway)
			.get();

		Assert.assertTrue("No JobGraph was submitted.", submittedJobGraphFuture.isDone());
		final JobGraph submittedJobGraph = submittedJobGraphFuture.get();
		Assert.assertEquals(1, submittedJobGraph.getUserJarBlobKeys().size());
	}
}

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

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link JobSubmitHandler}.
 */
public class JobSubmitHandlerTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
	private static BlobServer blobServer;

	@BeforeClass
	public static void setup() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			TEMPORARY_FOLDER.newFolder().getAbsolutePath());

		blobServer = new BlobServer(config, new VoidBlobStore());
		blobServer.start();
	}

	@AfterClass
	public static void teardown() throws IOException {
		if (blobServer != null) {
			blobServer.close();
		}
	}

	@Test
	public void testSerializationFailureHandling() throws Exception {
		final Path jobGraphFile = TEMPORARY_FOLDER.newFile().toPath();
		DispatcherGateway mockGateway = mock(DispatcherGateway.class);
		when(mockGateway.submitJob(any(JobGraph.class), any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			TestingUtils.defaultExecutor());

		JobSubmitRequestBody request = new JobSubmitRequestBody(jobGraphFile.toString(), Collections.emptyList(), Collections.emptyList());

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
		try (OutputStream fileOut = Files.newOutputStream(jobGraphFile)) {
			try (ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
				objectOut.writeObject(new JobGraph("testjob"));
			}
		}

		DispatcherGateway mockGateway = mock(DispatcherGateway.class);
		when(mockGateway.getHostname()).thenReturn("localhost");
		when(mockGateway.getBlobServerPort(any(Time.class))).thenReturn(CompletableFuture.completedFuture(blobServer.getPort()));
		when(mockGateway.submitJob(any(JobGraph.class), any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			TestingUtils.defaultExecutor());

		JobSubmitRequestBody request = new JobSubmitRequestBody(jobGraphFile.getFileName().toString(), Collections.emptyList(), Collections.emptyList());

		handler.handleRequest(new HandlerRequest<>(request, EmptyMessageParameters.getInstance(), Collections.emptyMap(), Collections.emptyMap(), Collections.singleton(jobGraphFile)), mockGateway)
			.get();
	}

	@Test
	public void testRejectionOnCountMismatch() throws Exception {
		final Path jobGraphFile = TEMPORARY_FOLDER.newFile().toPath();
		try (OutputStream fileOut = Files.newOutputStream(jobGraphFile)) {
			try (ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
				objectOut.writeObject(new JobGraph("testjob"));
			}
		}
		final Path countExceedingFile = TEMPORARY_FOLDER.newFile().toPath();

		DispatcherGateway mockGateway = mock(DispatcherGateway.class);
		when(mockGateway.getHostname()).thenReturn("localhost");
		when(mockGateway.getBlobServerPort(any(Time.class))).thenReturn(CompletableFuture.completedFuture(blobServer.getPort()));
		when(mockGateway.submitJob(any(JobGraph.class), any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			TestingUtils.defaultExecutor());

		JobSubmitRequestBody request = new JobSubmitRequestBody(jobGraphFile.getFileName().toString(), Collections.emptyList(), Collections.emptyList());

		try {
			handler.handleRequest(new HandlerRequest<>(request, EmptyMessageParameters.getInstance(), Collections.emptyMap(), Collections.emptyMap(), Arrays.asList(jobGraphFile, countExceedingFile)), mockGateway)
				.get();
		} catch (Exception e) {
			ExceptionUtils.findThrowable(e, candidate -> candidate instanceof RestHandlerException && candidate.getMessage().contains("count"));
		}
	}

	@Test
	public void testFileHandling() throws Exception {
		final String dcEntryName = "entry";

		CompletableFuture<JobGraph> submittedJobGraphFuture = new CompletableFuture<>();
		DispatcherGateway dispatcherGateway = new TestingDispatcherGateway.Builder()
			.setBlobServerPort(blobServer.getPort())
			.setSubmitFunction(submittedJobGraph -> {
				submittedJobGraphFuture.complete(submittedJobGraph);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.build();

		GatewayRetriever<DispatcherGateway> gatewayRetriever = new TestGatewayRetriever(dispatcherGateway);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			gatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			TestingUtils.defaultExecutor());

		final Path jobGraphFile = TEMPORARY_FOLDER.newFile().toPath();
		final Path jarFile = TEMPORARY_FOLDER.newFile().toPath();
		final Path artifactFile = TEMPORARY_FOLDER.newFile().toPath();

		final JobGraph jobGraph = new JobGraph();
		// the entry that should be updated
		jobGraph.addUserArtifact(dcEntryName, new DistributedCache.DistributedCacheEntry("random", false));
		try (OutputStream fileOut = Files.newOutputStream(jobGraphFile)) {
			try (ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
				objectOut.writeObject(jobGraph);
			}
		}

		JobSubmitRequestBody request = new JobSubmitRequestBody(
			jobGraphFile.getFileName().toString(),
			Collections.singletonList(jarFile.getFileName().toString()),
			Collections.singleton(new JobSubmitRequestBody.DistributedCacheFile(dcEntryName, artifactFile.getFileName().toString())));

		handler.handleRequest(new HandlerRequest<>(
				request,
				EmptyMessageParameters.getInstance(),
				Collections.emptyMap(),
				Collections.emptyMap(),
				Arrays.asList(jobGraphFile, jarFile, artifactFile)), dispatcherGateway)
			.get();

		Assert.assertTrue("No JobGraph was submitted.", submittedJobGraphFuture.isDone());
		final JobGraph submittedJobGraph = submittedJobGraphFuture.get();
		Assert.assertEquals(1, submittedJobGraph.getUserJarBlobKeys().size());
		Assert.assertEquals(1, submittedJobGraph.getUserArtifacts().size());
		Assert.assertNotNull(submittedJobGraph.getUserArtifacts().get(dcEntryName).blobKey);
	}

	private static class TestGatewayRetriever implements GatewayRetriever<DispatcherGateway> {

		private final DispatcherGateway dispatcherGateway;

		TestGatewayRetriever(DispatcherGateway dispatcherGateway) {
			this.dispatcherGateway = dispatcherGateway;
		}

		@Override
		public CompletableFuture<DispatcherGateway> getFuture() {
			return CompletableFuture.completedFuture(dispatcherGateway);
		}
	}
}

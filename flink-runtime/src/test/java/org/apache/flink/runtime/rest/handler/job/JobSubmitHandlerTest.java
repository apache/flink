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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link JobSubmitHandler}.
 */
public class JobSubmitHandlerTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

	/** The instance of the (non-ssl) BLOB server used during the tests. */
	private static BlobServer blobServer;

	@BeforeClass
	public static void startServer() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			TMP_FOLDER.newFolder().getAbsolutePath());

		blobServer = new BlobServer(config, new VoidBlobStore());
		blobServer.start();
	}

	@Test
	public void testSerializationFailureHandling() throws Exception {
		DispatcherGateway mockGateway = new JobGraphCapturingMockGateway();
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			new Configuration());

		JobSubmitRequestBody request = new JobSubmitRequestBody(new byte[0]);

		try {
			handler.handleRequest(new HandlerRequest<>(request, EmptyMessageParameters.getInstance()), mockGateway);
			Assert.fail();
		} catch (RestHandlerException rhe) {
			Assert.assertEquals(HttpResponseStatus.BAD_REQUEST, rhe.getHttpResponseStatus());
		}
	}

	@Test
	public void testSuccessfulJobSubmission() throws Exception {
		DispatcherGateway mockGateway = new JobGraphCapturingMockGateway();
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			new Configuration());

		JobGraph job = new JobGraph("testjob");
		JobSubmitRequestBody request = new JobSubmitRequestBody(job);

		handler.handleRequest(new HandlerRequest<>(request, EmptyMessageParameters.getInstance()), mockGateway)
			.get();
	}

	@Test
	public void testJarHandling() throws Exception {
		final String jarName = "jar";

		JobGraphCapturingMockGateway jobGraphCapturingMockGateway = new JobGraphCapturingMockGateway();
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			new Configuration());

		Path tmp = TMP_FOLDER.newFolder().toPath();
		Path clientStorageDirectory = Files.createDirectory(tmp.resolve("client-storage-directory"));
		Path serverStorageDirectory = Files.createDirectory(tmp.resolve("server-storage-directory"));

		Path jar = Paths.get(jarName);
		Files.createFile(clientStorageDirectory.resolve(jar));
		Files.createFile(serverStorageDirectory.resolve(jar));

		JobGraph job = new JobGraph("testjob");
		job.addJar(new org.apache.flink.core.fs.Path(jar.toUri()));
		JobSubmitRequestBody serializedJobGraphBody = new JobSubmitRequestBody(job);
		JobSubmitRequestBody request = new JobSubmitRequestBody(serializedJobGraphBody.serializedJobGraph, Collections.singletonList(serverStorageDirectory.resolve(jar)), Collections.emptyList(), serverStorageDirectory);

		handler.handleRequest(new HandlerRequest<>(request, EmptyMessageParameters.getInstance()), jobGraphCapturingMockGateway)
			.get();

		JobGraph submittedJobGraph = jobGraphCapturingMockGateway.jobGraph;
		List<org.apache.flink.core.fs.Path> userJars = submittedJobGraph.getUserJars();

		// ensure we haven't changed the total number of jars
		Assert.assertEquals(1, userJars.size());

		// this entry should be changed, a replacement jar exists in the server storage directory
		Assert.assertEquals(new org.apache.flink.core.fs.Path(serverStorageDirectory.resolve(jar).toUri()), userJars.get(0));
	}

	@Test
	public void testArtifactHandling() throws Exception {
		final String remoteEntryName = "remote";
		final String uploadEntryName = "upload";

		JobGraphCapturingMockGateway jobGraphCapturingMockGateway = new JobGraphCapturingMockGateway();
		GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

		JobSubmitHandler handler = new JobSubmitHandler(
			CompletableFuture.completedFuture("http://localhost:1234"),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT,
			Collections.emptyMap(),
			new Configuration());

		Path tmp = TMP_FOLDER.newFolder().toPath();
		Path clientStorageDirectory = Files.createDirectory(tmp.resolve("client-storage-directory"));
		Path serverStorageDirectory = Files.createDirectory(tmp.resolve("server-storage-directory"));

		Path remoteFile = Paths.get(remoteEntryName);
		Files.createFile(clientStorageDirectory.resolve(remoteFile));
		Path uploadFile = Paths.get(uploadEntryName);
		Files.createFile(clientStorageDirectory.resolve(uploadFile));
		Files.createFile(serverStorageDirectory.resolve(uploadFile));

		JobGraph job = new JobGraph("testjob");
		job.addUserArtifact("remote", new DistributedCache.DistributedCacheEntry(clientStorageDirectory.resolve(remoteFile).toString(), false));
		job.addUserArtifact("upload", new DistributedCache.DistributedCacheEntry(clientStorageDirectory.resolve(uploadFile).toString(), false));
		JobSubmitRequestBody serializedJobGraphBody = new JobSubmitRequestBody(job);
		JobSubmitRequestBody request = new JobSubmitRequestBody(serializedJobGraphBody.serializedJobGraph, Collections.emptyList(), Collections.singletonList(serverStorageDirectory.resolve(uploadFile)), serverStorageDirectory);

		handler.handleRequest(new HandlerRequest<>(request, EmptyMessageParameters.getInstance()), jobGraphCapturingMockGateway)
			.get();

		JobGraph submittedJobGraph = jobGraphCapturingMockGateway.jobGraph;
		Map<String, DistributedCache.DistributedCacheEntry> userArtifacts = submittedJobGraph.getUserArtifacts();

		// ensure we haven't changed the total number of artifacts
		Assert.assertEquals(2, userArtifacts.size());

		// this entry should be unchanged, no replacement file exists in the server storage directory
		DistributedCache.DistributedCacheEntry remoteEntry = userArtifacts.get(remoteEntryName);
		Assert.assertNotNull(remoteEntry);
		Assert.assertEquals(clientStorageDirectory.resolve(remoteFile).toString(), remoteEntry.filePath);

		// this entry should be changed, a replacement file exists in the server storage directory
		DistributedCache.DistributedCacheEntry uploadEntry = userArtifacts.get(uploadEntryName);
		Assert.assertNotNull(uploadEntry);
		Assert.assertEquals(serverStorageDirectory.resolve(uploadFile).toString(), uploadEntry.filePath);
	}

	private static class JobGraphCapturingMockGateway implements DispatcherGateway {

		private JobGraph jobGraph;

		@Override
		public String getAddress() {
			return "akka.tcp://localhost";
		}

		@Override
		public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
			this.jobGraph = jobGraph;
			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		@Override
		public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
			return CompletableFuture.completedFuture(blobServer.getPort());
		}

		@Override
		public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public CompletableFuture<Acknowledge> stopJob(JobID jobId, Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public CompletableFuture<String> requestRestAddress(Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public CompletableFuture<Collection<String>> requestMetricQueryServicePaths(Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(Time timeout) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public DispatcherId getFencingToken() {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public String getHostname() {
			throw new UnsupportedOperationException("Should not be called.");
		}
	}
}

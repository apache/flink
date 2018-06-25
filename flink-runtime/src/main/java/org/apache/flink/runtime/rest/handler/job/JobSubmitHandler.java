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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * This handler can be used to submit jobs to a Flink cluster.
 */
public final class JobSubmitHandler extends AbstractRestHandler<DispatcherGateway, JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {

	private static final String FILE_TYPE_GRAPH = "JobGraph";
	private static final String FILE_TYPE_JAR = "Jar";
	private static final String FILE_TYPE_ARTIFACT = "Artifact";

	public JobSubmitHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers) {
		super(localRestAddress, leaderRetriever, timeout, headers, JobSubmitHeaders.getInstance());
	}

	@Override
	protected CompletableFuture<JobSubmitResponseBody> handleRequest(@Nonnull HandlerRequest<JobSubmitRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
		Collection<Path> uploadedFiles = request.getUploadedFiles();
		Map<String, Path> nameToFile = uploadedFiles.stream().collect(Collectors.toMap(
			path -> path.getFileName().toString(),
			entry -> entry
		));

		JobSubmitRequestBody requestBody = request.getRequestBody();

		Path jobGraphFile = getPathAndAssertUpload(requestBody.jobGraphFileName, FILE_TYPE_GRAPH, nameToFile);

		Collection<org.apache.flink.core.fs.Path> jarFiles = new ArrayList<>(requestBody.jarFileNames.size());
		for (String jarFileName : requestBody.jarFileNames) {
			Path jarFile = getPathAndAssertUpload(jarFileName, FILE_TYPE_JAR, nameToFile);
			jarFiles.add(new org.apache.flink.core.fs.Path(jarFile.toString()));
		}

		Collection<Tuple2<String, org.apache.flink.core.fs.Path>> artifacts = new ArrayList<>(requestBody.artifactFileNames.size());
		for (JobSubmitRequestBody.DistributedCacheFile artifactFileName : requestBody.artifactFileNames) {
			Path artifactFile = getPathAndAssertUpload(artifactFileName.fileName, FILE_TYPE_ARTIFACT, nameToFile);
			artifacts.add(Tuple2.of(artifactFileName.entryName, new org.apache.flink.core.fs.Path(artifactFile.toString())));
		}

		// TODO: use executor
		CompletableFuture<JobGraph> jobGraphFuture = CompletableFuture.supplyAsync(() -> {
			JobGraph jobGraph;
			try (ObjectInputStream objectIn = new ObjectInputStream(Files.newInputStream(jobGraphFile))) {
				jobGraph = (JobGraph) objectIn.readObject();
			} catch (Exception e) {
				throw new CompletionException(new RestHandlerException(
					"Failed to deserialize JobGraph.",
					HttpResponseStatus.BAD_REQUEST,
					e));
			}
			return jobGraph;
		});

		CompletableFuture<Integer> blobServerPortFuture = gateway.getBlobServerPort(timeout);

		CompletableFuture<JobGraph> finalizedJobGraphFuture = jobGraphFuture.thenCombine(blobServerPortFuture, (jobGraph, blobServerPort) -> {
			final InetSocketAddress address = new InetSocketAddress(gateway.getHostname(), blobServerPort);
			try (BlobClient blobClient = new BlobClient(address, new Configuration())){
				Collection<PermanentBlobKey> jarBlobKeys = ClientUtils.uploadUserJars(jobGraph.getJobID(), jarFiles, blobClient);
				ClientUtils.setUserJarBlobKeys(jarBlobKeys, jobGraph);
				Collection<Tuple2<String, PermanentBlobKey>> artifactBlobKeys = ClientUtils.uploadUserArtifacts(jobGraph.getJobID(), artifacts, blobClient);
				ClientUtils.setUserArtifactBlobKeys(jobGraph, artifactBlobKeys);
			} catch (IOException e) {
				throw new CompletionException(new RestHandlerException(
					"Could not upload job files.",
					HttpResponseStatus.INTERNAL_SERVER_ERROR,
					e));
			}
			return jobGraph;
		});

		CompletableFuture<Acknowledge> jobSubmissionFuture = finalizedJobGraphFuture.thenCompose(jobGraph -> gateway.submitJob(jobGraph, timeout));

		return jobSubmissionFuture.thenCombine(jobGraphFuture,
			(ack, jobGraph) -> new JobSubmitResponseBody("/jobs/" + jobGraph.getJobID()));
	}

	private static Path getPathAndAssertUpload(String fileName, String type, Map<String, Path> uploadedFiles) throws MissingFileException {
		Path file = uploadedFiles.get(fileName);
		if (file == null) {
			throw new MissingFileException(type, fileName);
		}
		return file;
	}

	private static final class MissingFileException extends RestHandlerException {

		private static final long serialVersionUID = -7954810495610194965L;

		MissingFileException(String type, String fileName) {
			super(type + " file " + fileName + " could not be found on the server.", HttpResponseStatus.BAD_REQUEST);
		}
	}
}

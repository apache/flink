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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
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
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * This handler can be used to submit jobs to a Flink cluster.
 */
public final class JobSubmitHandler extends AbstractRestHandler<DispatcherGateway, JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {

	private static final String FILE_TYPE_JOB_GRAPH = "JobGraph";
	private static final String FILE_TYPE_JAR = "Jar";
	private static final String FILE_TYPE_ARTIFACT = "Artifact";

	private final Executor executor;
	private final Configuration configuration;

	public JobSubmitHandler(
			GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers,
			Executor executor,
			Configuration configuration) {
		super(leaderRetriever, timeout, headers, JobSubmitHeaders.getInstance());
		this.executor = executor;
		this.configuration = configuration;
	}

	@Override
	protected CompletableFuture<JobSubmitResponseBody> handleRequest(@Nonnull HandlerRequest<JobSubmitRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
		final Collection<File> uploadedFiles = request.getUploadedFiles();
		final Map<String, Path> nameToFile = uploadedFiles.stream().collect(Collectors.toMap(
			File::getName,
			Path::fromLocalFile
		));

		if (uploadedFiles.size() != nameToFile.size()) {
			throw new RestHandlerException(
				String.format("The number of uploaded files was %s than the expected count. Expected: %s Actual %s",
					uploadedFiles.size() < nameToFile.size() ? "lower" : "higher",
					nameToFile.size(),
					uploadedFiles.size()),
				HttpResponseStatus.BAD_REQUEST
			);
		}

		final JobSubmitRequestBody requestBody = request.getRequestBody();

		if (requestBody.jobGraphFileName == null) {
			throw new RestHandlerException(
				String.format("The %s field must not be omitted or be null.",
					JobSubmitRequestBody.FIELD_NAME_JOB_GRAPH),
				HttpResponseStatus.BAD_REQUEST);
		}

		CompletableFuture<JobGraph> jobGraphFuture = loadJobGraph(requestBody, nameToFile);

		Collection<Path> jarFiles = getJarFilesToUpload(requestBody.jarFileNames, nameToFile);

		Collection<Tuple2<String, Path>> artifacts = getArtifactFilesToUpload(requestBody.artifactFileNames, nameToFile);

		CompletableFuture<JobGraph> finalizedJobGraphFuture = uploadJobGraphFiles(gateway, jobGraphFuture, jarFiles, artifacts, configuration);

		CompletableFuture<Acknowledge> jobSubmissionFuture = finalizedJobGraphFuture.thenCompose(jobGraph -> gateway.submitJob(jobGraph, timeout));

		return jobSubmissionFuture.thenCombine(jobGraphFuture,
			(ack, jobGraph) -> new JobSubmitResponseBody("/jobs/" + jobGraph.getJobID()));
	}

	private CompletableFuture<JobGraph> loadJobGraph(JobSubmitRequestBody requestBody, Map<String, Path> nameToFile) throws MissingFileException {
		final Path jobGraphFile = getPathAndAssertUpload(requestBody.jobGraphFileName, FILE_TYPE_JOB_GRAPH, nameToFile);

		return CompletableFuture.supplyAsync(() -> {
			JobGraph jobGraph;
			try (ObjectInputStream objectIn = new ObjectInputStream(jobGraphFile.getFileSystem().open(jobGraphFile))) {
				jobGraph = (JobGraph) objectIn.readObject();
			} catch (Exception e) {
				throw new CompletionException(new RestHandlerException(
					"Failed to deserialize JobGraph.",
					HttpResponseStatus.BAD_REQUEST,
					e));
			}
			return jobGraph;
		}, executor);
	}

	private static Collection<Path> getJarFilesToUpload(Collection<String> jarFileNames, Map<String, Path> nameToFileMap) throws MissingFileException {
		Collection<Path> jarFiles = new ArrayList<>(jarFileNames.size());
		for (String jarFileName : jarFileNames) {
			Path jarFile = getPathAndAssertUpload(jarFileName, FILE_TYPE_JAR, nameToFileMap);
			jarFiles.add(new Path(jarFile.toString()));
		}
		return jarFiles;
	}

	private static Collection<Tuple2<String, Path>> getArtifactFilesToUpload(
			Collection<JobSubmitRequestBody.DistributedCacheFile> artifactEntries,
			Map<String, Path> nameToFileMap) throws MissingFileException {
		Collection<Tuple2<String, Path>> artifacts = new ArrayList<>(artifactEntries.size());
		for (JobSubmitRequestBody.DistributedCacheFile artifactFileName : artifactEntries) {
			Path artifactFile = getPathAndAssertUpload(artifactFileName.fileName, FILE_TYPE_ARTIFACT, nameToFileMap);
			artifacts.add(Tuple2.of(artifactFileName.entryName, new Path(artifactFile.toString())));
		}
		return artifacts;
	}

	private CompletableFuture<JobGraph> uploadJobGraphFiles(
			DispatcherGateway gateway,
			CompletableFuture<JobGraph> jobGraphFuture,
			Collection<Path> jarFiles,
			Collection<Tuple2<String, Path>> artifacts,
			Configuration configuration) {
		CompletableFuture<Integer> blobServerPortFuture = gateway.getBlobServerPort(timeout);

		return jobGraphFuture.thenCombine(blobServerPortFuture, (JobGraph jobGraph, Integer blobServerPort) -> {
			final InetSocketAddress address = new InetSocketAddress(gateway.getHostname(), blobServerPort);
			try {
				ClientUtils.uploadJobGraphFiles(jobGraph, jarFiles, artifacts, () -> new BlobClient(address, configuration));
			} catch (FlinkException e) {
				throw new CompletionException(new RestHandlerException(
					"Could not upload job files.",
					HttpResponseStatus.INTERNAL_SERVER_ERROR,
					e));
			}
			return jobGraph;
		});
	}

	private static Path getPathAndAssertUpload(String fileName, String type, Map<String, Path> uploadedFiles) throws MissingFileException {
		final Path file = uploadedFiles.get(fileName);
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

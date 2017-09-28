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

package org.apache.flink.client.program.rest;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.BlobServerPortHeaders;
import org.apache.flink.runtime.rest.messages.BlobServerPortResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.JobTerminationMessageParameters;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * A {@link ClusterClient} implementation that communicates via HTTP REST requests.
 */
public class RestClusterClient extends ClusterClient {

	private RestClusterClientConfiguration configuration;
	private final RestClient restEndpoint;

	public RestClusterClient(Configuration config) throws Exception {
		this(config, RestClusterClientConfiguration.fromConfiguration(config));
	}

	public RestClusterClient(Configuration config, RestClusterClientConfiguration configuration) throws Exception {
		super(config);
		this.configuration = configuration;
		this.restEndpoint = new RestClient(configuration.getRestEndpointConfiguration(), Executors.newFixedThreadPool(4));
	}

	@Override
	public void shutdown() {
		this.restEndpoint.shutdown(Time.seconds(5));
	}

	@Override
	protected JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		log.info("Submitting job.");
		try {
			// temporary hack for FLIP-6
			jobGraph.setAllowQueuedScheduling(true);
			submitJob(jobGraph);
		} catch (JobSubmissionException e) {
			throw new RuntimeException(e);
		}
		// don't return just a JobSubmissionResult here, the signature is lying
		// The CliFrontend expects this to be a JobExecutionResult

		// TOOD: do not exit this method until job is finished
		return new JobExecutionResult(jobGraph.getJobID(), 1, Collections.emptyMap());
	}

	private void submitJob(JobGraph jobGraph) throws JobSubmissionException {
		log.info("Requesting blob server port.");
		int blobServerPort;
		try {
			CompletableFuture<BlobServerPortResponseBody> portFuture = restEndpoint.sendRequest(
				configuration.getRestServerAddress(),
				configuration.getRestServerPort(),
				BlobServerPortHeaders.getInstance());
			blobServerPort = portFuture.get().port;
		} catch (Exception e) {
			throw new JobSubmissionException(jobGraph.getJobID(), "Failed to retrieve blob server port.", e);
		}

		log.info("Uploading jar files.");
		try {
			InetSocketAddress address = new InetSocketAddress(configuration.getBlobServerAddress(), blobServerPort);
			List<BlobKey> keys = BlobClient.uploadJarFiles(address, new Configuration(), jobGraph.getJobID(), jobGraph.getUserJars());
			for (BlobKey key : keys) {
				jobGraph.addBlob(key);
			}
		} catch (Exception e) {
			throw new JobSubmissionException(jobGraph.getJobID(), "Failed to upload user jars to blob server.", e);
		}

		log.info("Submitting job graph.");
		try {
			CompletableFuture<JobSubmitResponseBody> responseFuture = restEndpoint.sendRequest(
				configuration.getRestServerAddress(),
				configuration.getRestServerPort(),
				JobSubmitHeaders.getInstance(),
				new JobSubmitRequestBody(jobGraph));
			JobSubmitResponseBody response = responseFuture.get();
			System.out.println(response.jobUrl);
		} catch (Exception e) {
			throw new JobSubmissionException(jobGraph.getJobID(), "Failed to submit JobGraph.", e);
		}
	}

	@Override
	public void stop(JobID jobID) throws Exception {
		JobTerminationMessageParameters param = new JobTerminationMessageParameters();
		param.jobPathParameter.resolve(jobID);
		param.terminationModeQueryParameter.resolve(Collections.singletonList(TerminationModeQueryParameter.TerminationMode.STOP));
		CompletableFuture<EmptyResponseBody> responseFuture = restEndpoint.sendRequest(
			configuration.getRestServerAddress(),
			configuration.getRestServerPort(),
			JobTerminationHeaders.getInstance(),
			param
		);
		responseFuture.get();
		System.out.println("Job stopping initiated.");
	}

	@Override
	public void cancel(JobID jobID) throws Exception {
		JobTerminationMessageParameters param = new JobTerminationMessageParameters();
		param.jobPathParameter.resolve(jobID);
		param.terminationModeQueryParameter.resolve(Collections.singletonList(TerminationModeQueryParameter.TerminationMode.CANCEL));
		CompletableFuture<EmptyResponseBody> responseFuture = restEndpoint.sendRequest(
			configuration.getRestServerAddress(),
			configuration.getRestServerPort(),
			JobTerminationHeaders.getInstance(),
			param
		);
		responseFuture.get();
		System.out.println("Job canceling initiated.");
	}

	// ======================================
	// Legacy stuff we actually implement
	// ======================================

	@Override
	public String getClusterIdentifier() {
		return "Flip-6 Standalone cluster with dispatcher at " + configuration.getRestServerAddress();
	}

	@Override
	public boolean hasUserJarsInClassPath(List<URL> userJarFiles) {
		return false;
	}

	// ======================================
	// Legacy stuff we ignore
	// ======================================

	@Override
	public void waitForClusterToBeReady() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getWebInterfaceURL() {
		throw new UnsupportedOperationException();
	}

	@Override
	public GetClusterStatusResponse getClusterStatus() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected List<String> getNewMessages() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void finalizeCluster() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getMaxSlots() {
		return 0;
	}
}

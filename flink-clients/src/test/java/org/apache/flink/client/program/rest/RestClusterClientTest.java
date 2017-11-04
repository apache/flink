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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.messages.BlobServerPortHeaders;
import org.apache.flink.runtime.rest.messages.BlobServerPortResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.JobTerminationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTargetDirectoryParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerResponseBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RestClusterClient}.
 *
 * <p>These tests verify that the client uses the appropriate headers for each
 * request, properly constructs the request bodies/parameters and processes the responses correctly.
 */
public class RestClusterClientTest extends TestLogger {

	private static final String restAddress = "http://localhost:1234";
	private static final Dispatcher mockRestfulGateway = mock(Dispatcher.class);
	private static final GatewayRetriever<DispatcherGateway> mockGatewayRetriever = mock(GatewayRetriever.class);

	static {
		when(mockRestfulGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(restAddress));
		when(mockGatewayRetriever.getNow()).thenReturn(Optional.of(mockRestfulGateway));
	}

	@Test
	public void testJobSubmitCancelStop() throws Exception {

		Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");

		RestServerEndpointConfiguration rsec = RestServerEndpointConfiguration.fromConfiguration(config);

		TestBlobServerPortHandler portHandler = new TestBlobServerPortHandler();
		TestJobSubmitHandler submitHandler = new TestJobSubmitHandler();
		TestJobTerminationHandler terminationHandler = new TestJobTerminationHandler();

		RestServerEndpoint rse = new RestServerEndpoint(rsec) {
			@Override
			protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {

				List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>();
				handlers.add(Tuple2.of(portHandler.getMessageHeaders(), portHandler));
				handlers.add(Tuple2.of(submitHandler.getMessageHeaders(), submitHandler));
				handlers.add(Tuple2.of(terminationHandler.getMessageHeaders(), terminationHandler));
				return handlers;
			}
		};

		RestClusterClient rcc = new RestClusterClient(config);
		try {
			rse.start();

			JobGraph job = new JobGraph("testjob");
			JobID id = job.getJobID();

			Assert.assertFalse(portHandler.portRetrieved);
			Assert.assertFalse(submitHandler.jobSubmitted);
			rcc.submitJob(job, ClassLoader.getSystemClassLoader());
			Assert.assertTrue(portHandler.portRetrieved);
			Assert.assertTrue(submitHandler.jobSubmitted);

			Assert.assertFalse(terminationHandler.jobCanceled);
			rcc.cancel(id);
			Assert.assertTrue(terminationHandler.jobCanceled);

			Assert.assertFalse(terminationHandler.jobStopped);
			rcc.stop(id);
			Assert.assertTrue(terminationHandler.jobStopped);

		} finally {
			rcc.shutdown();
			rse.shutdown(Time.seconds(5));
		}
	}

	private static class TestBlobServerPortHandler extends TestHandler<EmptyRequestBody, BlobServerPortResponseBody, EmptyMessageParameters> {
		private volatile boolean portRetrieved = false;

		private TestBlobServerPortHandler() {
			super(BlobServerPortHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<BlobServerPortResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			portRetrieved = true;
			return CompletableFuture.completedFuture(new BlobServerPortResponseBody(12000));
		}
	}

	private static class TestJobSubmitHandler extends TestHandler<JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {
		private volatile boolean jobSubmitted = false;

		private TestJobSubmitHandler() {
			super(JobSubmitHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<JobSubmitResponseBody> handleRequest(@Nonnull HandlerRequest<JobSubmitRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			jobSubmitted = true;
			return CompletableFuture.completedFuture(new JobSubmitResponseBody("/url"));
		}
	}

	private static class TestJobTerminationHandler extends TestHandler<EmptyRequestBody, EmptyResponseBody, JobTerminationMessageParameters> {
		private volatile boolean jobCanceled = false;
		private volatile boolean jobStopped = false;

		private TestJobTerminationHandler() {
			super(JobTerminationHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<EmptyResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, JobTerminationMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			switch (request.getQueryParameter(TerminationModeQueryParameter.class).get(0)) {
				case CANCEL:
					jobCanceled = true;
					break;
				case STOP:
					jobStopped = true;
					break;
			}
			return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
		}
	}

	@Test
	public void testTriggerSavepoint() throws Exception {

		Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");

		RestServerEndpointConfiguration rsec = RestServerEndpointConfiguration.fromConfiguration(config);

		String targetSavepointDirectory = "/alternate";

		TestSavepointTriggerHandler triggerHandler = new TestSavepointTriggerHandler(targetSavepointDirectory);

		RestServerEndpoint rse = new RestServerEndpoint(rsec) {
			@Override
			protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {

				List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>();
				handlers.add(Tuple2.of(triggerHandler.getMessageHeaders(), triggerHandler));
				return handlers;
			}
		};

		RestClusterClient rcc = new RestClusterClient(config);
		try {
			rse.start();

			JobID id = new JobID();

			{
				CompletableFuture<String> savepointPathFuture = rcc.triggerSavepoint(id, null);
				String savepointPath = savepointPathFuture.get();
				Assert.assertEquals("/universe", savepointPath);
			}

			{
				CompletableFuture<String> savepointPathFuture = rcc.triggerSavepoint(id, targetSavepointDirectory);
				String savepointPath = savepointPathFuture.get();
				Assert.assertEquals(targetSavepointDirectory + "/universe", savepointPath);
			}
		} finally {
			rcc.shutdown();
			rse.shutdown(Time.seconds(5));
		}
	}

	private static class TestSavepointTriggerHandler extends TestHandler<EmptyRequestBody, SavepointTriggerResponseBody, SavepointMessageParameters> {

		private final String expectedSavepointDirectory;

		TestSavepointTriggerHandler(String expectedSavepointDirectory) {
			super(SavepointTriggerHeaders.getInstance());
			this.expectedSavepointDirectory = expectedSavepointDirectory;
		}

		@Override
		protected CompletableFuture<SavepointTriggerResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, SavepointMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			List<String> targetDirectories = request.getQueryParameter(SavepointTargetDirectoryParameter.class);
			if (targetDirectories.isEmpty()) {
				return CompletableFuture.completedFuture(new SavepointTriggerResponseBody("growing", "/universe", "big-bang"));
			} else {
				String targetDir = targetDirectories.get(0);
				if (targetDir.equals(expectedSavepointDirectory)) {
					return CompletableFuture.completedFuture(new SavepointTriggerResponseBody("growing", targetDir + "/universe", "big-bang"));
				} else {
					return CompletableFuture.completedFuture(new SavepointTriggerResponseBody("growing", "savepoint directory (" + targetDir + ") did not match expected (" + expectedSavepointDirectory + ')', "big-bang"));
				}
			}
		}
	}

	@Test
	public void testListJobs() throws Exception {

		Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");

		RestServerEndpointConfiguration rsec = RestServerEndpointConfiguration.fromConfiguration(config);

		TestListJobsHandler listJobsHandler = new TestListJobsHandler();

		RestServerEndpoint rse = new RestServerEndpoint(rsec) {
			@Override
			protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {

				List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>();
				handlers.add(Tuple2.of(listJobsHandler.getMessageHeaders(), listJobsHandler));
				return handlers;
			}
		};

		RestClusterClient rcc = new RestClusterClient(config);
		try {
			rse.start();

			{
				CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture = rcc.listJobs();
				Collection<JobStatusMessage> jobDetails = jobDetailsFuture.get();
				Iterator<JobStatusMessage> jobDetailsIterator = jobDetails.iterator();
				JobStatusMessage job1 = jobDetailsIterator.next();
				JobStatusMessage job2 = jobDetailsIterator.next();
				Assert.assertNotEquals("The job statues should not be equal.", job1.getJobState(), job2.getJobState());
			}
		} finally {
			rcc.shutdown();
			rse.shutdown(Time.seconds(5));
		}}

	private static class TestListJobsHandler extends TestHandler<EmptyRequestBody, MultipleJobsDetails, EmptyMessageParameters> {

		private TestListJobsHandler() {
			super(JobsOverviewHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<MultipleJobsDetails> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			JobDetails running = new JobDetails(new JobID(), "job1", 0, 0, 0, JobStatus.RUNNING, 0, new int[9], 0);
			JobDetails finished = new JobDetails(new JobID(), "job2", 0, 0, 0, JobStatus.FINISHED, 0, new int[9], 0);
			return CompletableFuture.completedFuture(new MultipleJobsDetails(Arrays.asList(running, finished)));
		}
	}

	private abstract static class TestHandler<R extends RequestBody, P extends ResponseBody, M extends MessageParameters> extends AbstractRestHandler<DispatcherGateway, R, P, M> {

		private TestHandler(MessageHeaders<R, P, M> headers) {
			super(
				CompletableFuture.completedFuture(restAddress),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT,
				Collections.emptyMap(),
				headers);
		}
	}
}

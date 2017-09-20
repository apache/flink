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
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
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
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RestClusterClient}.
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
			protected Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {

				Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>();
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

	private static class TestBlobServerPortHandler extends AbstractRestHandler<DispatcherGateway, EmptyRequestBody, BlobServerPortResponseBody, EmptyMessageParameters> {
		private volatile boolean portRetrieved = false;

		private TestBlobServerPortHandler() {
			super(
				CompletableFuture.completedFuture(restAddress),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT,
				BlobServerPortHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<BlobServerPortResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			portRetrieved = true;
			return CompletableFuture.completedFuture(new BlobServerPortResponseBody(12000));
		}
	}

	private static class TestJobSubmitHandler extends AbstractRestHandler<DispatcherGateway, JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {
		private volatile boolean jobSubmitted = false;

		private TestJobSubmitHandler() {
			super(
				CompletableFuture.completedFuture(restAddress),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT,
				JobSubmitHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<JobSubmitResponseBody> handleRequest(@Nonnull HandlerRequest<JobSubmitRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			jobSubmitted = true;
			return CompletableFuture.completedFuture(new JobSubmitResponseBody("/url"));
		}
	}

	private static class TestJobTerminationHandler extends AbstractRestHandler<DispatcherGateway, EmptyRequestBody, EmptyResponseBody, JobTerminationMessageParameters> {
		private volatile boolean jobCanceled = false;
		private volatile boolean jobStopped = false;

		private TestJobTerminationHandler() {
			super(
				CompletableFuture.completedFuture(restAddress),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT,
				JobTerminationHeaders.getInstance());
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
}

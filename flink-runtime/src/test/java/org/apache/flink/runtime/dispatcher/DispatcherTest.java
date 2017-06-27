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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneHaServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for the {@link Dispatcher} component.
 */
public class DispatcherTest extends TestLogger {

	/**
	 * Tests that we can submit a job to the Dispatcher which then spawns a
	 * new JobManagerRunner.
	 */
	@Test
	public void testJobSubmission() throws Exception {
		TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
		RpcService rpcService = new TestingRpcService();
		HighAvailabilityServices haServices = new StandaloneHaServices("localhost", "localhost");
		HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 10000L);
		JobManagerRunner jobManagerRunner = mock(JobManagerRunner.class);

		final Time timeout = Time.seconds(5L);
		final JobGraph jobGraph = mock(JobGraph.class);
		final JobID jobId = new JobID();
		when(jobGraph.getJobID()).thenReturn(jobId);

		try {
			final TestingDispatcher dispatcher = new TestingDispatcher(
				rpcService,
				Dispatcher.DISPATCHER_NAME,
				new Configuration(),
				haServices,
				mock(BlobServer.class),
				heartbeatServices,
				mock(MetricRegistry.class),
				fatalErrorHandler,
				jobManagerRunner,
				jobId);

			dispatcher.start();

			DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

			CompletableFuture<Acknowledge> acknowledgeFuture = dispatcherGateway.submitJob(jobGraph, timeout);

			acknowledgeFuture.get();

			verify(jobManagerRunner, Mockito.timeout(timeout.toMilliseconds())).start();

			// check that no error has occurred
			fatalErrorHandler.rethrowError();
		} finally {
			rpcService.stopService();
		}
	}

	private static class TestingDispatcher extends Dispatcher {

		private final JobManagerRunner jobManagerRunner;
		private final JobID expectedJobId;

		protected TestingDispatcher(
				RpcService rpcService,
				String endpointId,
				Configuration configuration,
				HighAvailabilityServices highAvailabilityServices,
				BlobServer blobServer,
				HeartbeatServices heartbeatServices,
				MetricRegistry metricRegistry,
				FatalErrorHandler fatalErrorHandler,
				JobManagerRunner jobManagerRunner,
				JobID expectedJobId) throws Exception {
			super(
				rpcService,
				endpointId,
				configuration,
				highAvailabilityServices,
				blobServer,
				heartbeatServices,
				metricRegistry,
				fatalErrorHandler);

			this.jobManagerRunner = jobManagerRunner;
			this.expectedJobId = expectedJobId;
		}

		@Override
		protected JobManagerRunner createJobManagerRunner(
				ResourceID resourceId,
				JobGraph jobGraph,
				Configuration configuration,
				RpcService rpcService,
				HighAvailabilityServices highAvailabilityServices,
				BlobServer blobServer,
				HeartbeatServices heartbeatServices,
				MetricRegistry metricRegistry,
				OnCompletionActions onCompleteActions,
				FatalErrorHandler fatalErrorHandler) throws Exception {
			assertEquals(expectedJobId, jobGraph.getJobID());

			return jobManagerRunner;
		}
	}
}

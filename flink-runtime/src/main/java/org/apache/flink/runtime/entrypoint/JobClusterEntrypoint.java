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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRestEndpoint;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;

/**
 * Base class for per-job cluster entry points.
 */
public abstract class JobClusterEntrypoint extends ClusterEntrypoint {

	private ResourceManager<?> resourceManager;

	private JobManagerServices jobManagerServices;

	private JobMasterRestEndpoint jobMasterRestEndpoint;

	private LeaderRetrievalService jobMasterRetrievalService;

	private LeaderRetrievalService resourceManagerRetrievalService;

	private JobManagerRunner jobManagerRunner;

	public JobClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected void startClusterComponents(
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry) throws Exception {

		resourceManager = createResourceManager(
			configuration,
			ResourceID.generate(),
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			metricRegistry,
			this,
			null);

		jobManagerServices = JobManagerServices.fromConfiguration(configuration, blobServer);

		resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

		final LeaderGatewayRetriever<JobMasterGateway> jobMasterGatewayRetriever = new RpcGatewayRetriever<>(
			rpcService,
			JobMasterGateway.class,
			JobMasterId::new,
			10,
			Time.milliseconds(50L));

		final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
			rpcService,
			ResourceManagerGateway.class,
			ResourceManagerId::new,
			10,
			Time.milliseconds(50L));

		// TODO: Remove once we have ported the MetricFetcher to the RpcEndpoint
		final ActorSystem actorSystem = ((AkkaRpcService) rpcService).getActorSystem();
		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

		jobMasterRestEndpoint = createJobMasterRestEndpoint(
			configuration,
			jobMasterGatewayRetriever,
			resourceManagerGatewayRetriever,
			rpcService.getExecutor(),
			new AkkaQueryServiceRetriever(actorSystem, timeout));

		LOG.debug("Starting JobMaster REST endpoint.");
		jobMasterRestEndpoint.start();

		jobManagerRunner = createJobManagerRunner(
			configuration,
			ResourceID.generate(),
			rpcService,
			highAvailabilityServices,
			jobManagerServices,
			heartbeatServices,
			metricRegistry,
			this,
			jobMasterRestEndpoint.getRestAddress());

		LOG.debug("Starting ResourceManager.");
		resourceManager.start();
		resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);

		LOG.debug("Starting JobManager.");
		jobManagerRunner.start();

		jobMasterRetrievalService = highAvailabilityServices.getJobManagerLeaderRetriever(
			jobManagerRunner.getJobGraph().getJobID(),
			jobManagerRunner.getAddress());
		jobMasterRetrievalService.start(jobMasterGatewayRetriever);
	}

	protected JobMasterRestEndpoint createJobMasterRestEndpoint(
			Configuration configuration,
			GatewayRetriever<JobMasterGateway> jobMasterGatewayRetriever,
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			Executor executor,
			MetricQueryServiceRetriever metricQueryServiceRetriever) throws ConfigurationException {

		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(configuration);

		return new JobMasterRestEndpoint(
			RestServerEndpointConfiguration.fromConfiguration(configuration),
			jobMasterGatewayRetriever,
			configuration,
			restHandlerConfiguration,
			resourceManagerGatewayRetriever,
			executor,
			metricQueryServiceRetriever);

	}

	protected JobManagerRunner createJobManagerRunner(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			JobManagerServices jobManagerServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String restAddress) throws Exception {

		final JobGraph jobGraph = retrieveJobGraph(configuration);

		return new JobManagerRunner(
			resourceId,
			jobGraph,
			configuration,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			jobManagerServices,
			metricRegistry,
			new TerminatingOnCompleteActions(jobGraph.getJobID()),
			fatalErrorHandler,
			restAddress);
	}

	@Override
	protected void stopClusterComponents(boolean cleanupHaData) throws Exception {
		Throwable exception = null;

		if (jobMasterRestEndpoint != null) {
			jobMasterRestEndpoint.shutdown(Time.seconds(10L));
		}

		if (jobMasterRetrievalService != null) {
			try {
				jobMasterRetrievalService.stop();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}
		}

		if (jobManagerRunner != null) {
			try {
				jobManagerRunner.shutdown();
			} catch (Throwable t) {
				exception = t;
			}
		}

		if (jobManagerServices != null) {
			try {
				jobManagerServices.shutdown();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}
		}

		if (resourceManagerRetrievalService != null) {
			try {
				resourceManagerRetrievalService.stop();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}
		}

		if (resourceManager != null) {
			try {
				resourceManager.shutDown();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}
		}

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the job cluster entry point.", exception);
		}
	}

	private void shutDownAndTerminate(boolean cleanupHaData) {
		try {
			shutDown(cleanupHaData);
		} catch (Throwable t) {
			LOG.error("Could not properly shut down cluster entrypoint.", t);
		}

		System.exit(SUCCESS_RETURN_CODE);
	}

	protected abstract ResourceManager<?> createResourceManager(
		Configuration configuration,
		ResourceID resourceId,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		MetricRegistry metricRegistry,
		FatalErrorHandler fatalErrorHandler,
		@Nullable String webInterfaceUrl) throws Exception;

	protected abstract JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException;

	private final class TerminatingOnCompleteActions implements OnCompletionActions {

		private final JobID jobId;

		private TerminatingOnCompleteActions(JobID jobId) {
			this.jobId = Preconditions.checkNotNull(jobId);
		}

		@Override
		public void jobFinished(JobExecutionResult result) {
			LOG.info("Job({}) finished.", jobId);

			shutDownAndTerminate(true);
		}

		@Override
		public void jobFailed(Throwable cause) {
			LOG.info("Job({}) failed.", jobId, cause);

			shutDownAndTerminate(false);
		}

		@Override
		public void jobFinishedByOther() {
			LOG.info("Job({}) was finished by another JobManager.", jobId);

			shutDownAndTerminate(false);
		}
	}
}

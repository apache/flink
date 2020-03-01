/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.SchedulerNGFactoryFactory;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.jobmaster.TestingJobManagerSharedServicesBuilder;
import org.apache.flink.runtime.jobmaster.factories.UnregisteredJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import java.util.concurrent.CompletableFuture;

/**
 * A builder for the {@link JobMaster}.
 */
public class JobMasterBuilder {

	private static final long heartbeatInterval = 1000L;
	private static final long heartbeatTimeout = 5_000_000L;
	private static final HeartbeatServices DEFAULT_HEARTBEAT_SERVICES = new HeartbeatServices(heartbeatInterval, heartbeatTimeout);

	private Configuration configuration = new Configuration();

	private final JobGraph jobGraph;
	private final RpcService rpcService;

	private HighAvailabilityServices highAvailabilityServices;

	private JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();

	private HeartbeatServices heartbeatServices = DEFAULT_HEARTBEAT_SERVICES;

	private SlotPoolFactory slotPoolFactory = null;

	private SchedulerFactory schedulerFactory = null;

	private OnCompletionActions onCompletionActions = new TestingOnCompletionActions();

	private ShuffleMaster<?> shuffleMaster = NettyShuffleMaster.INSTANCE;

	private PartitionTrackerFactory partitionTrackerFactory = NoOpJobMasterPartitionTracker.FACTORY;

	private ResourceID jmResourceId = ResourceID.generate();

	private FatalErrorHandler fatalErrorHandler = error -> {
	};

	public JobMasterBuilder(JobGraph jobGraph, RpcService rpcService) {
		TestingHighAvailabilityServices testingHighAvailabilityServices = new TestingHighAvailabilityServices();
		testingHighAvailabilityServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

		SettableLeaderRetrievalService rmLeaderRetrievalService = new SettableLeaderRetrievalService(
			null,
			null);
		testingHighAvailabilityServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);

		this.highAvailabilityServices = testingHighAvailabilityServices;
		this.jobGraph = jobGraph;
		this.rpcService = rpcService;
	}

	public JobMasterBuilder withConfiguration(Configuration configuration) {
		this.configuration = configuration;
		return this;
	}

	public JobMasterBuilder withHighAvailabilityServices(HighAvailabilityServices highAvailabilityServices) {
		this.highAvailabilityServices = highAvailabilityServices;
		return this;
	}

	public JobMasterBuilder withJobManagerSharedServices(JobManagerSharedServices jobManagerSharedServices) {
		this.jobManagerSharedServices = jobManagerSharedServices;
		return this;
	}

	public JobMasterBuilder withHeartbeatServices(HeartbeatServices heartbeatServices) {
		this.heartbeatServices = heartbeatServices;
		return this;
	}

	public JobMasterBuilder withSlotPoolFactory(SlotPoolFactory slotPoolFactory) {
		this.slotPoolFactory = slotPoolFactory;
		return this;
	}

	public JobMasterBuilder withFatalErrorHandler(FatalErrorHandler fatalErrorHandler) {
		this.fatalErrorHandler = fatalErrorHandler;
		return this;
	}

	public JobMasterBuilder withSchedulerFactory(SchedulerFactory schedulerFactory) {
		this.schedulerFactory = schedulerFactory;
		return this;
	}

	public JobMasterBuilder withOnCompletionActions(OnCompletionActions onCompletionActions) {
		this.onCompletionActions = onCompletionActions;
		return this;
	}

	public JobMasterBuilder withResourceId(ResourceID resourceId) {
		this.jmResourceId = resourceId;
		return this;
	}

	public JobMasterBuilder withShuffleMaster(ShuffleMaster<?> shuffleMaster) {
		this.shuffleMaster = shuffleMaster;
		return this;
	}

	public JobMasterBuilder withPartitionTrackerFactory(PartitionTrackerFactory partitionTrackerFactory) {
		this.partitionTrackerFactory = partitionTrackerFactory;
		return this;
	}

	public JobMaster createJobMaster() throws Exception {
		final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

		return new JobMaster(
			rpcService,
			jobMasterConfiguration,
			jmResourceId,
			jobGraph,
			highAvailabilityServices,
			slotPoolFactory != null ? slotPoolFactory : DefaultSlotPoolFactory.fromConfiguration(configuration),
			schedulerFactory != null ? schedulerFactory : DefaultSchedulerFactory.fromConfiguration(configuration),
			jobManagerSharedServices,
			heartbeatServices,
			UnregisteredJobManagerJobMetricGroupFactory.INSTANCE,
			onCompletionActions,
			fatalErrorHandler,
			JobMasterBuilder.class.getClassLoader(),
			SchedulerNGFactoryFactory.createSchedulerNGFactory(configuration, jobManagerSharedServices.getRestartStrategyFactory()),
			shuffleMaster,
			partitionTrackerFactory);
	}

	/**
	 * Test {@link OnCompletionActions} implementation that exposes a {@link CompletableFuture} for every method which
	 * is completed with the method argument once that method is called.
	 */
	public static final class TestingOnCompletionActions implements OnCompletionActions {

		private final CompletableFuture<ArchivedExecutionGraph> jobReachedGloballyTerminalStateFuture = new CompletableFuture<>();
		private final CompletableFuture<Void> jobFinishedByOtherFuture = new CompletableFuture<>();
		private final CompletableFuture<Throwable> jobMasterFailedFuture = new CompletableFuture<>();

		@Override
		public void jobReachedGloballyTerminalState(ArchivedExecutionGraph executionGraph) {
			jobReachedGloballyTerminalStateFuture.complete(executionGraph);
		}

		@Override
		public void jobFinishedByOther() {
			jobFinishedByOtherFuture.complete(null);
		}

		@Override
		public void jobMasterFailed(Throwable cause) {
			jobMasterFailedFuture.complete(cause);
		}

		public CompletableFuture<ArchivedExecutionGraph> getJobReachedGloballyTerminalStateFuture() {
			return jobReachedGloballyTerminalStateFuture;
		}

		public CompletableFuture<ArchivedExecutionGraph> getJobFinishedByOtherFuture() {
			return jobReachedGloballyTerminalStateFuture;
		}

		public CompletableFuture<ArchivedExecutionGraph> getJobMasterFailedFuture() {
			return jobReachedGloballyTerminalStateFuture;
		}
	}
}

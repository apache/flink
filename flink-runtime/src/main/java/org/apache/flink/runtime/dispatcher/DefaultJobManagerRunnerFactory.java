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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleServiceLoader;

/**
 * Singleton default factory for {@link JobManagerRunner}.
 */
public enum DefaultJobManagerRunnerFactory implements JobManagerRunnerFactory {
	INSTANCE;

	@Override
	public JobManagerRunner createJobManagerRunner(
			JobGraph jobGraph,
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			JobManagerSharedServices jobManagerServices,
			JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

		final SlotPoolFactory slotPoolFactory = DefaultSlotPoolFactory.fromConfiguration(configuration);
		final SchedulerFactory schedulerFactory = DefaultSchedulerFactory.fromConfiguration(configuration);
		final SchedulerNGFactory schedulerNGFactory = SchedulerNGFactoryFactory.createSchedulerNGFactory(configuration, jobManagerServices.getRestartStrategyFactory());
		final ShuffleMaster<?> shuffleMaster = ShuffleServiceLoader.loadShuffleServiceFactory(configuration).createShuffleMaster(configuration);

		final JobMasterServiceFactory jobMasterFactory = new DefaultJobMasterServiceFactory(
			jobMasterConfiguration,
			slotPoolFactory,
			schedulerFactory,
			rpcService,
			highAvailabilityServices,
			jobManagerServices,
			heartbeatServices,
			jobManagerJobMetricGroupFactory,
			fatalErrorHandler,
			schedulerNGFactory,
			shuffleMaster);

		return new JobManagerRunner(
			jobGraph,
			jobMasterFactory,
			highAvailabilityServices,
			jobManagerServices.getLibraryCacheManager(),
			jobManagerServices.getScheduledExecutorService(),
			fatalErrorHandler);
	}
}

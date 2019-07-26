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

package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerImpl;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

/**
 * Default implementation of the {@link JobMasterServiceFactory}.
 */
public class DefaultJobMasterServiceFactory implements JobMasterServiceFactory {

	private final JobMasterConfiguration jobMasterConfiguration;

	private final SlotPoolFactory slotPoolFactory;

	private final SchedulerFactory schedulerFactory;

	private final RpcService rpcService;

	private final HighAvailabilityServices haServices;

	private final JobManagerSharedServices jobManagerSharedServices;

	private final HeartbeatServices heartbeatServices;

	private final JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory;

	private final FatalErrorHandler fatalErrorHandler;

	private final SchedulerNGFactory schedulerNGFactory;

	private final ShuffleMaster<?> shuffleMaster;

	public DefaultJobMasterServiceFactory(
			JobMasterConfiguration jobMasterConfiguration,
			SlotPoolFactory slotPoolFactory,
			SchedulerFactory schedulerFactory,
			RpcService rpcService,
			HighAvailabilityServices haServices,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices,
			JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
			FatalErrorHandler fatalErrorHandler,
			SchedulerNGFactory schedulerNGFactory,
			ShuffleMaster<?> shuffleMaster) {
		this.jobMasterConfiguration = jobMasterConfiguration;
		this.slotPoolFactory = slotPoolFactory;
		this.schedulerFactory = schedulerFactory;
		this.rpcService = rpcService;
		this.haServices = haServices;
		this.jobManagerSharedServices = jobManagerSharedServices;
		this.heartbeatServices = heartbeatServices;
		this.jobManagerJobMetricGroupFactory = jobManagerJobMetricGroupFactory;
		this.fatalErrorHandler = fatalErrorHandler;
		this.schedulerNGFactory = schedulerNGFactory;
		this.shuffleMaster = shuffleMaster;
	}

	@Override
	public JobMaster createJobMasterService(
			JobGraph jobGraph,
			OnCompletionActions jobCompletionActions,
			ClassLoader userCodeClassloader) throws Exception {

		return new JobMaster(
			rpcService,
			jobMasterConfiguration,
			ResourceID.generate(),
			jobGraph,
			haServices,
			slotPoolFactory,
			schedulerFactory,
			jobManagerSharedServices,
			heartbeatServices,
			jobManagerJobMetricGroupFactory,
			jobCompletionActions,
			fatalErrorHandler,
			userCodeClassloader,
			schedulerNGFactory,
			shuffleMaster,
			lookup -> new PartitionTrackerImpl(
				jobGraph.getJobID(),
				shuffleMaster,
				lookup
			));
	}
}

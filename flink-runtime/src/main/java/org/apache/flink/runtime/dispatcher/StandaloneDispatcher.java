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
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * Dispatcher implementation which spawns a {@link JobMaster} for each
 * submitted {@link JobGraph} within in the same process. This dispatcher
 * can be used as the default for all different session clusters.
 */
public class StandaloneDispatcher extends Dispatcher {
	protected StandaloneDispatcher(
			RpcService rpcService,
			String endpointId,
			Configuration configuration,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		super(
			rpcService,
			endpointId,
			configuration,
			highAvailabilityServices,
			blobServer,
			heartbeatServices,
			metricRegistry,
			fatalErrorHandler);
	}

	@Override
	protected JobManagerRunner createJobManagerRunner(
			ResourceID resourceId,
			JobGraph jobGraph,
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobService blobService,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			OnCompletionActions onCompleteActions,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		// create the standard job manager runner
		return new JobManagerRunner(
			resourceId,
			jobGraph,
			configuration,
			rpcService,
			highAvailabilityServices,
			blobService,
			heartbeatServices,
			metricRegistry,
			onCompleteActions,
			fatalErrorHandler);
	}
}

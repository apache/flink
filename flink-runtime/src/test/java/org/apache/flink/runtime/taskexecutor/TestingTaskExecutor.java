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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;


import java.util.concurrent.CompletableFuture;

/**
 * {@link TaskExecutor} extension for testing purposes.
 */
class TestingTaskExecutor extends TaskExecutor {
	private final CompletableFuture<Void> startFuture = new CompletableFuture<>();

	public TestingTaskExecutor(
			RpcService rpcService,
			TaskManagerConfiguration taskManagerConfiguration,
			HighAvailabilityServices haServices,
			TaskManagerServices taskExecutorServices,
			HeartbeatServices heartbeatServices,
			TaskManagerMetricGroup taskManagerMetricGroup,
			String metricQueryServiceAddress,
			BlobCacheService blobCacheService,
			FatalErrorHandler fatalErrorHandler,
			TaskExecutorPartitionTracker partitionTracker,
			BackPressureSampleService backPressureSampleService) {
		super(
			rpcService,
			taskManagerConfiguration,
			haServices,
			taskExecutorServices,
			heartbeatServices,
			taskManagerMetricGroup,
			metricQueryServiceAddress,
			blobCacheService,
			fatalErrorHandler,
			partitionTracker,
			backPressureSampleService);
	}

	@Override
	public void onStart() throws Exception {
		try {
			super.onStart();
		} catch (Exception e) {
			startFuture.completeExceptionally(e);
			throw e;
		}

		startFuture.complete(null);
	}

	void waitUntilStarted() {
		startFuture.join();
	}
}

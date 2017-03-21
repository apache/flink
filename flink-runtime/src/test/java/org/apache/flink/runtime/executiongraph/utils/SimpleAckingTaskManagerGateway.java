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

package org.apache.flink.runtime.executiongraph.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTrace;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;

import java.util.UUID;

/**
 * A TaskManagerGateway that simply acks the basic operations (deploy, cancel, update) and does not
 * support any more advanced operations.
 */
public class SimpleAckingTaskManagerGateway implements TaskManagerGateway {

	private final String address = UUID.randomUUID().toString();

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public void disconnectFromJobManager(InstanceID instanceId, Exception cause) {}

	@Override
	public void stopCluster(ApplicationStatus applicationStatus, String message) {}

	@Override
	public Future<StackTrace> requestStackTrace(Time timeout) {
		return FlinkCompletableFuture.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public Future<StackTraceSampleResponse> requestStackTraceSample(
			ExecutionAttemptID executionAttemptID,
			int sampleId,
			int numSamples,
			Time delayBetweenSamples,
			int maxStackTraceDepth,
			Time timeout) {
		return FlinkCompletableFuture.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public Future<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
		return FlinkCompletableFuture.completed(Acknowledge.get());
	}

	@Override
	public Future<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return FlinkCompletableFuture.completed(Acknowledge.get());
	}

	@Override
	public Future<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return FlinkCompletableFuture.completed(Acknowledge.get());
	}

	@Override
	public Future<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		return FlinkCompletableFuture.completed(Acknowledge.get());
	}

	@Override
	public void failPartition(ExecutionAttemptID executionAttemptID) {}

	@Override
	public void notifyCheckpointComplete(
			ExecutionAttemptID executionAttemptID,
			JobID jobId,
			long checkpointId,
			long timestamp) {}

	@Override
	public void triggerCheckpoint(
			ExecutionAttemptID executionAttemptID,
			JobID jobId,
			long checkpointId,
			long timestamp,
			CheckpointOptions checkpointOptions) {}

	@Override
	public Future<BlobKey> requestTaskManagerLog(Time timeout) {
		return FlinkCompletableFuture.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public Future<BlobKey> requestTaskManagerStdout(Time timeout) {
		return FlinkCompletableFuture.completedExceptionally(new UnsupportedOperationException());
	}
}

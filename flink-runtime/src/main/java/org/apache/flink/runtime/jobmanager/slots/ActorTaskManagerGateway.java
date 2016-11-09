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

package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.Messages;
import org.apache.flink.runtime.messages.StackTrace;
import org.apache.flink.runtime.messages.StackTraceSampleMessages;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.messages.TaskMessages;
import org.apache.flink.runtime.messages.checkpoint.NotifyCheckpointComplete;
import org.apache.flink.runtime.messages.checkpoint.TriggerCheckpoint;
import org.apache.flink.util.Preconditions;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

/**
 * Implementation of the {@link TaskManagerGateway} for {@link ActorGateway}.
 */
public class ActorTaskManagerGateway implements TaskManagerGateway {
	private final ActorGateway actorGateway;

	public ActorTaskManagerGateway(ActorGateway actorGateway) {
		this.actorGateway = Preconditions.checkNotNull(actorGateway);
	}

	public ActorGateway getActorGateway() {
		return actorGateway;
	}

	//-------------------------------------------------------------------------------
	// Task manager rpc methods
	//-------------------------------------------------------------------------------

	@Override
	public String getAddress() {
		return actorGateway.path();
	}

	@Override
	public void disconnectFromJobManager(InstanceID instanceId, Exception cause) {
		actorGateway.tell(new Messages.Disconnect(instanceId, cause));
	}

	@Override
	public void stopCluster(final ApplicationStatus applicationStatus, final String message) {
		actorGateway.tell(new StopCluster(applicationStatus, message));
	}

	@Override
	public Future<StackTrace> requestStackTrace(final Time timeout) {
		Preconditions.checkNotNull(timeout);

		scala.concurrent.Future<StackTrace> stackTraceFuture = actorGateway.ask(
			TaskManagerMessages.SendStackTrace$.MODULE$.get(),
			new FiniteDuration(timeout.getSize(), timeout.getUnit()))
			.mapTo(ClassTag$.MODULE$.<StackTrace>apply(StackTrace.class));

		return new FlinkFuture<>(stackTraceFuture);
	}

	@Override
	public Future<StackTraceSampleResponse> requestStackTraceSample(
			ExecutionAttemptID executionAttemptID,
			int sampleId,
			int numSamples,
			Time delayBetweenSamples,
			int maxStackTraceDepth,
			Time timeout) {
		Preconditions.checkNotNull(executionAttemptID);
		Preconditions.checkArgument(numSamples > 0, "The number of samples must be greater than 0.");
		Preconditions.checkNotNull(delayBetweenSamples);
		Preconditions.checkArgument(maxStackTraceDepth >= 0, "The max stack trace depth must be greater or equal than 0.");
		Preconditions.checkNotNull(timeout);

		scala.concurrent.Future<StackTraceSampleResponse> stackTraceSampleResponseFuture = actorGateway.ask(
			new StackTraceSampleMessages.TriggerStackTraceSample(
				sampleId,
				executionAttemptID,
				numSamples,
				delayBetweenSamples,
				maxStackTraceDepth),
			new FiniteDuration(timeout.getSize(), timeout.getUnit()))
			.mapTo(ClassTag$.MODULE$.<StackTraceSampleResponse>apply(StackTraceSampleResponse.class));

		return new FlinkFuture<>(stackTraceSampleResponseFuture);
	}

	@Override
	public Future<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
		Preconditions.checkNotNull(tdd);
		Preconditions.checkNotNull(timeout);

		scala.concurrent.Future<Acknowledge> submitResult = actorGateway.ask(
			new TaskMessages.SubmitTask(tdd),
			new FiniteDuration(timeout.getSize(), timeout.getUnit()))
			.mapTo(ClassTag$.MODULE$.<Acknowledge>apply(Acknowledge.class));

		return new FlinkFuture<>(submitResult);
	}

	@Override
	public Future<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		Preconditions.checkNotNull(executionAttemptID);
		Preconditions.checkNotNull(timeout);

		scala.concurrent.Future<Acknowledge> stopResult = actorGateway.ask(
			new TaskMessages.StopTask(executionAttemptID),
			new FiniteDuration(timeout.getSize(), timeout.getUnit()))
			.mapTo(ClassTag$.MODULE$.<Acknowledge>apply(Acknowledge.class));

		return new FlinkFuture<>(stopResult);
	}

	@Override
	public Future<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		Preconditions.checkNotNull(executionAttemptID);
		Preconditions.checkNotNull(timeout);

		scala.concurrent.Future<Acknowledge> cancelResult = actorGateway.ask(
			new TaskMessages.CancelTask(executionAttemptID),
			new FiniteDuration(timeout.getSize(), timeout.getUnit()))
			.mapTo(ClassTag$.MODULE$.<Acknowledge>apply(Acknowledge.class));

		return new FlinkFuture<>(cancelResult);
	}

	@Override
	public Future<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		Preconditions.checkNotNull(executionAttemptID);
		Preconditions.checkNotNull(partitionInfos);

		TaskMessages.UpdatePartitionInfo updatePartitionInfoMessage = new TaskMessages.UpdateTaskMultiplePartitionInfos(
			executionAttemptID,
			partitionInfos);

		scala.concurrent.Future<Acknowledge> updatePartitionsResult = actorGateway.ask(
			updatePartitionInfoMessage,
			new FiniteDuration(timeout.getSize(), timeout.getUnit()))
			.mapTo(ClassTag$.MODULE$.<Acknowledge>apply(Acknowledge.class));

		return new FlinkFuture<>(updatePartitionsResult);
	}

	@Override
	public void failPartition(ExecutionAttemptID executionAttemptID) {
		Preconditions.checkNotNull(executionAttemptID);

		actorGateway.tell(new TaskMessages.FailIntermediateResultPartitions(executionAttemptID));
	}

	@Override
	public void notifyCheckpointComplete(
			ExecutionAttemptID executionAttemptID,
			JobID jobId,
			long checkpointId,
			long timestamp) {

		Preconditions.checkNotNull(executionAttemptID);
		Preconditions.checkNotNull(jobId);

		actorGateway.tell(new NotifyCheckpointComplete(jobId, executionAttemptID, checkpointId, timestamp));
	}

	@Override
	public void triggerCheckpoint(
			ExecutionAttemptID executionAttemptID,
			JobID jobId,
			long checkpointId,
			long timestamp) {

		Preconditions.checkNotNull(executionAttemptID);
		Preconditions.checkNotNull(jobId);

		actorGateway.tell(new TriggerCheckpoint(jobId, executionAttemptID, checkpointId, timestamp));
	}

	@Override
	public Future<BlobKey> requestTaskManagerLog(Time timeout) {
		return requestTaskManagerLog((TaskManagerMessages.RequestTaskManagerLog) TaskManagerMessages.getRequestTaskManagerLog(), timeout);
	}

	@Override
	public Future<BlobKey> requestTaskManagerStdout(Time timeout) {
		return requestTaskManagerLog((TaskManagerMessages.RequestTaskManagerLog) TaskManagerMessages.getRequestTaskManagerStdout(), timeout);
	}

	private Future<BlobKey> requestTaskManagerLog(TaskManagerMessages.RequestTaskManagerLog request, Time timeout) {
		Preconditions.checkNotNull(request);
		Preconditions.checkNotNull(timeout);

		scala.concurrent.Future<BlobKey> blobKeyFuture = actorGateway
			.ask(
				request,
				new FiniteDuration(timeout.getSize(), timeout.getUnit()))
			.mapTo(ClassTag$.MODULE$.<BlobKey>apply(BlobKey.class));

		return new FlinkFuture<>(blobKeyFuture);
	}
}

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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A wrapper of result partition writer for handling the logic of consumable notification.
 *
 * <p>Before a consuming task can request the result, it has to be deployed. The time of deployment
 * depends on the PIPELINED vs. BLOCKING characteristic of the result partition. With pipelined
 * results, receivers are deployed as soon as the first buffer is added to the result partition.
 * With blocking results on the other hand, receivers are deployed after the partition is finished.
 */
public class ConsumableNotifyingResultPartitionWriterDecorator implements ResultPartitionWriter, CheckpointedResultPartition {

	private final TaskActions taskActions;

	private final JobID jobId;

	private final ResultPartitionWriter partitionWriter;

	private final ResultPartitionConsumableNotifier partitionConsumableNotifier;

	private boolean hasNotifiedPipelinedConsumers;

	public ConsumableNotifyingResultPartitionWriterDecorator(
			TaskActions taskActions,
			JobID jobId,
			ResultPartitionWriter partitionWriter,
			ResultPartitionConsumableNotifier partitionConsumableNotifier) {
		this.taskActions = checkNotNull(taskActions);
		this.jobId = checkNotNull(jobId);
		this.partitionWriter = checkNotNull(partitionWriter);
		this.partitionConsumableNotifier = checkNotNull(partitionConsumableNotifier);
	}

	@Override
	public ResultPartitionID getPartitionId() {
		return partitionWriter.getPartitionId();
	}

	@Override
	public int getNumberOfSubpartitions() {
		return partitionWriter.getNumberOfSubpartitions();
	}

	@Override
	public int getNumTargetKeyGroups() {
		return partitionWriter.getNumTargetKeyGroups();
	}

	@Override
	public void setup() throws IOException {
		partitionWriter.setup();
	}

	@Override
	public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
		partitionWriter.emitRecord(record, targetSubpartition);

		notifyPipelinedConsumers();
	}

	@Override
	public void broadcastRecord(ByteBuffer record) throws IOException {
		partitionWriter.broadcastRecord(record);

		notifyPipelinedConsumers();
	}

	@Override
	public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
		partitionWriter.broadcastEvent(event, isPriorityEvent);

		notifyPipelinedConsumers();
	}

	@Override
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		partitionWriter.setMetricGroup(metrics);
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(int index, BufferAvailabilityListener availabilityListener) throws IOException {
		return partitionWriter.createSubpartitionView(index, availabilityListener);
	}

	@Override
	public void flushAll() {
		partitionWriter.flushAll();
	}

	@Override
	public void flush(int subpartitionIndex) {
		partitionWriter.flush(subpartitionIndex);
	}

	@Override
	public void finish() throws IOException {
		partitionWriter.finish();

		notifyPipelinedConsumers();
	}

	@Override
	public boolean isFinished() {
		return partitionWriter.isFinished();
	}

	@Override
	public void release(Throwable cause) {
		partitionWriter.release(cause);
	}

	@Override
	public boolean isReleased() {
		return partitionWriter.isReleased();
	}

	@Override
	public void fail(Throwable throwable) {
		partitionWriter.fail(throwable);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return partitionWriter.getAvailableFuture();
	}

	@Override
	public void close() throws Exception {
		partitionWriter.close();
	}

	/**
	 * Notifies pipelined consumers of this result partition once.
	 *
	 * <p>For PIPELINED {@link org.apache.flink.runtime.io.network.partition.ResultPartitionType}s,
	 * this will trigger the deployment of consuming tasks after the first buffer has been added.
	 */
	private void notifyPipelinedConsumers() {
		if (!hasNotifiedPipelinedConsumers && !partitionWriter.isReleased()) {
			partitionConsumableNotifier.notifyPartitionConsumable(jobId, partitionWriter.getPartitionId(), taskActions);

			hasNotifiedPipelinedConsumers = true;
		}
	}

	// ------------------------------------------------------------------------
	//  checkpointable methods
	// ------------------------------------------------------------------------

	@Override
	public CheckpointedResultSubpartition getCheckpointedSubpartition(int subpartitionIndex) {
		return getCheckpointablePartition().getCheckpointedSubpartition(subpartitionIndex);
	}

	@Override
	public void readRecoveredState(ChannelStateReader stateReader) throws IOException, InterruptedException {
		getCheckpointablePartition().readRecoveredState(stateReader);
	}

	private CheckpointedResultPartition getCheckpointablePartition() {
		if (partitionWriter instanceof CheckpointedResultPartition) {
			return (CheckpointedResultPartition) partitionWriter;
		} else {
			throw new IllegalStateException("This partition is not checkpointable: " + partitionWriter);
		}
	}

	// ------------------------------------------------------------------------
	//  Factory
	// ------------------------------------------------------------------------

	public static ResultPartitionWriter[] decorate(
			Collection<ResultPartitionDeploymentDescriptor> descs,
			ResultPartitionWriter[] partitionWriters,
			TaskActions taskActions,
			JobID jobId,
			ResultPartitionConsumableNotifier notifier) {

		ResultPartitionWriter[] consumableNotifyingPartitionWriters = new ResultPartitionWriter[partitionWriters.length];
		int counter = 0;
		for (ResultPartitionDeploymentDescriptor desc : descs) {
			if (desc.sendScheduleOrUpdateConsumersMessage() && desc.getPartitionType().isPipelined()) {
				consumableNotifyingPartitionWriters[counter] = new ConsumableNotifyingResultPartitionWriterDecorator(
					taskActions,
					jobId,
					partitionWriters[counter],
					notifier);
			} else {
				consumableNotifyingPartitionWriters[counter] = partitionWriters[counter];
			}
			counter++;
		}
		return consumableNotifyingPartitionWriters;
	}
}

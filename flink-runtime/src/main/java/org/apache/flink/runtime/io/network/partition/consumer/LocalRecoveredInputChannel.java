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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An input channel reads recovered state from previous unaligned checkpoint snapshots
 * via {@link ChannelStateReader} and then converts into {@link LocalInputChannel} finally.
 */
public class LocalRecoveredInputChannel extends RecoveredInputChannel {
	private final ResultPartitionManager partitionManager;
	private final TaskEventPublisher taskEventPublisher;

	LocalRecoveredInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			ResultPartitionManager partitionManager,
			TaskEventPublisher taskEventPublisher,
			int initialBackOff,
			int maxBackoff,
			InputChannelMetrics metrics) {
		super(inputGate, channelIndex, partitionId, initialBackOff, maxBackoff, metrics.getNumBytesInLocalCounter(), metrics.getNumBuffersInLocalCounter());

		this.partitionManager = checkNotNull(partitionManager);
		this.taskEventPublisher = checkNotNull(taskEventPublisher);
	}

	@Override
	public InputChannel toInputChannel() {
		return new LocalInputChannel(
			inputGate,
			getChannelIndex(),
			partitionId,
			partitionManager,
			taskEventPublisher,
			initialBackoff,
			maxBackoff,
			numBytesIn,
			numBytesIn);
	}
}

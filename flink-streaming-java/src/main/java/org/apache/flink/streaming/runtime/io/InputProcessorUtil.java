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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.streaming.api.graph.StreamConfig;

import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Utility for creating {@link CheckpointedInputGate} based on checkpoint mode
 * for {@link StreamOneInputProcessor} and {@link StreamTwoInputProcessor}.
 */
@Internal
public class InputProcessorUtil {

	public static CheckpointedInputGate createCheckpointedInputGate(
			AbstractInvokable toNotifyOnCheckpoint,
			StreamConfig config,
			ChannelStateWriter channelStateWriter,
			InputGate inputGate,
			Configuration taskManagerConfig,
			TaskIOMetricGroup taskIOMetricGroup,
			String taskName) {

		int pageSize = ConfigurationParserUtils.getPageSize(taskManagerConfig);

		BufferStorage bufferStorage = createBufferStorage(config, pageSize, taskManagerConfig, taskName);
		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			config,
			IntStream.of(inputGate.getNumberOfInputChannels()),
			channelStateWriter,
			taskName,
			toNotifyOnCheckpoint);
		registerCheckpointMetrics(taskIOMetricGroup, barrierHandler);

		barrierHandler.getBufferReceivedListener().ifPresent(inputGate::registerBufferReceivedListener);

		return new CheckpointedInputGate(inputGate, bufferStorage, barrierHandler);
	}

	/**
	 * @return a pair of {@link CheckpointedInputGate} created for two corresponding
	 * {@link InputGate}s supplied as parameters.
	 */
	public static CheckpointedInputGate[] createCheckpointedInputGatePair(
			AbstractInvokable toNotifyOnCheckpoint,
			StreamConfig config,
			ChannelStateWriter channelStateWriter,
			Configuration taskManagerConfig,
			TaskIOMetricGroup taskIOMetricGroup,
			String taskName,
			InputGate ...inputGates) {

		int pageSize = ConfigurationParserUtils.getPageSize(taskManagerConfig);

		BufferStorage[] mainBufferStorages = new BufferStorage[inputGates.length];
		for (int i = 0; i < inputGates.length; i++) {
			mainBufferStorages[i] = createBufferStorage(config, pageSize, taskManagerConfig, taskName);
		}

		BufferStorage[] linkedBufferStorages = new BufferStorage[inputGates.length];

		for (int i = 0; i < inputGates.length; i++) {
			linkedBufferStorages[i] = new LinkedBufferStorage(
				mainBufferStorages[i],
				mainBufferStorages[i].getMaxBufferedBytes(),
				copyBufferStoragesExceptOf(i, mainBufferStorages));
		}

		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			config,
			Arrays.stream(inputGates).mapToInt(InputGate::getNumberOfInputChannels),
			channelStateWriter,
			taskName,
			toNotifyOnCheckpoint);
		registerCheckpointMetrics(taskIOMetricGroup, barrierHandler);

		barrierHandler.getBufferReceivedListener().ifPresent(listener -> {
			for (final InputGate inputGate : inputGates) {
				inputGate.registerBufferReceivedListener(listener);
			}
		});

		CheckpointedInputGate[] checkpointedInputGates = new CheckpointedInputGate[inputGates.length];

		int channelIndexOffset = 0;
		for (int i = 0; i < inputGates.length; i++) {
			checkpointedInputGates[i] = new CheckpointedInputGate(inputGates[i], linkedBufferStorages[i], barrierHandler, channelIndexOffset);
			channelIndexOffset += inputGates[i].getNumberOfInputChannels();
		}

		return checkpointedInputGates;
	}

	private static BufferStorage[] copyBufferStoragesExceptOf(
			int skipStorage,
			BufferStorage[] bufferStorages) {
		BufferStorage[] copy = new BufferStorage[bufferStorages.length - 1];
		System.arraycopy(bufferStorages, 0, copy, 0, skipStorage);
		System.arraycopy(bufferStorages, skipStorage + 1, copy, skipStorage, bufferStorages.length - skipStorage - 1);
		return copy;
	}

	private static CheckpointBarrierHandler createCheckpointBarrierHandler(
			StreamConfig config,
			IntStream numberOfInputChannelsPerGate,
			ChannelStateWriter channelStateWriter,
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint) {
		switch (config.getCheckpointMode()) {
			case EXACTLY_ONCE:
				if (config.isUnalignedCheckpointsEnabled()) {
					return new CheckpointBarrierUnaligner(
						numberOfInputChannelsPerGate.toArray(),
						channelStateWriter,
						taskName,
						toNotifyOnCheckpoint);
				}
				return new CheckpointBarrierAligner(numberOfInputChannelsPerGate.sum(), taskName, toNotifyOnCheckpoint);
			case AT_LEAST_ONCE:
				return new CheckpointBarrierTracker(numberOfInputChannelsPerGate.sum(), toNotifyOnCheckpoint);
			default:
				throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + config.getCheckpointMode());
		}
	}

	private static BufferStorage createBufferStorage(
			StreamConfig config,
			int pageSize,
			Configuration taskManagerConfig,
			String taskName) {
		switch (config.getCheckpointMode()) {
			case EXACTLY_ONCE:
				if (config.isUnalignedCheckpointsEnabled()) {
					return new EmptyBufferStorage();
				}
				long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
				if (!(maxAlign == -1 || maxAlign > 0)) {
					throw new IllegalConfigurationException(
						TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
							+ " must be positive or -1 (infinite)");
				}
				return new CachedBufferStorage(pageSize, maxAlign, taskName);
			case AT_LEAST_ONCE:
				return new EmptyBufferStorage();
			default:
				throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + config.getCheckpointMode());
		}
	}

	private static void registerCheckpointMetrics(TaskIOMetricGroup taskIOMetricGroup, CheckpointBarrierHandler barrierHandler) {
		taskIOMetricGroup.gauge(MetricNames.CHECKPOINT_ALIGNMENT_TIME, barrierHandler::getAlignmentDurationNanos);
		taskIOMetricGroup.gauge(MetricNames.CHECKPOINT_START_DELAY_TIME, barrierHandler::getCheckpointStartDelayNanos);
	}
}

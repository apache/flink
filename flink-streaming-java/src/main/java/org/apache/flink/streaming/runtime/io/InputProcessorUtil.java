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
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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
			IndexedInputGate[] inputGates,
			TaskIOMetricGroup taskIOMetricGroup,
			String taskName) {
		InputGate inputGate = InputGateUtil.createInputGate(inputGates);
		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			config,
			Arrays.stream(inputGates).mapToInt(InputGate::getNumberOfInputChannels),
			channelStateWriter,
			taskName,
			generateChannelIndexToInputGateMap(inputGate),
			generateInputGateToChannelIndexOffsetMap(inputGate),
			toNotifyOnCheckpoint);
		registerCheckpointMetrics(taskIOMetricGroup, barrierHandler);

		barrierHandler.getBufferReceivedListener().ifPresent(inputGate::registerBufferReceivedListener);

		return new CheckpointedInputGate(inputGate, barrierHandler);
	}

	/**
	 * @return a pair of {@link CheckpointedInputGate} created for two corresponding
	 * {@link InputGate}s supplied as parameters.
	 */
	public static CheckpointedInputGate[] createCheckpointedInputGatePair(
			AbstractInvokable toNotifyOnCheckpoint,
			StreamConfig config,
			ChannelStateWriter channelStateWriter,
			TaskIOMetricGroup taskIOMetricGroup,
			String taskName,
			Collection<IndexedInputGate> ...inputGates) {

		InputGate[] unionedInputGates = new InputGate[inputGates.length];
		for (int i = 0; i < inputGates.length; i++) {
			unionedInputGates[i] = InputGateUtil.createInputGate(inputGates[i].toArray(new IndexedInputGate[0]));
		}

		Map<InputGate, Integer> inputGateToChannelIndexOffset = generateInputGateToChannelIndexOffsetMap(unionedInputGates);

		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			config,
			Arrays.stream(inputGates).flatMapToInt(collection -> collection.stream().mapToInt(InputGate::getNumberOfInputChannels)),
			channelStateWriter,
			taskName,
			generateChannelIndexToInputGateMap(unionedInputGates),
			inputGateToChannelIndexOffset,
			toNotifyOnCheckpoint);
		registerCheckpointMetrics(taskIOMetricGroup, barrierHandler);

		barrierHandler.getBufferReceivedListener().ifPresent(listener -> {
			for (final InputGate inputGate : unionedInputGates) {
				inputGate.registerBufferReceivedListener(listener);
			}
		});

		CheckpointedInputGate[] checkpointedInputGates = new CheckpointedInputGate[unionedInputGates.length];

		for (int i = 0; i < unionedInputGates.length; i++) {
			checkpointedInputGates[i] = new CheckpointedInputGate(
				unionedInputGates[i], barrierHandler, inputGateToChannelIndexOffset.get(unionedInputGates[i]));
		}

		return checkpointedInputGates;
	}

	private static CheckpointBarrierHandler createCheckpointBarrierHandler(
			StreamConfig config,
			IntStream numberOfInputChannelsPerGate,
			ChannelStateWriter channelStateWriter,
			String taskName,
			InputGate[] channelIndexToInputGate,
			Map<InputGate, Integer> inputGateToChannelIndexOffset,
			AbstractInvokable toNotifyOnCheckpoint) {
		switch (config.getCheckpointMode()) {
			case EXACTLY_ONCE:
				if (config.isUnalignedCheckpointsEnabled()) {
					return new AlternatingCheckpointBarrierHandler(
						new CheckpointBarrierAligner(
							taskName,
							channelIndexToInputGate,
							inputGateToChannelIndexOffset,
							toNotifyOnCheckpoint),
						new CheckpointBarrierUnaligner(
							numberOfInputChannelsPerGate.toArray(),
							channelStateWriter,
							taskName,
							toNotifyOnCheckpoint),
						toNotifyOnCheckpoint);
				}
				return new CheckpointBarrierAligner(
					taskName,
					channelIndexToInputGate,
					inputGateToChannelIndexOffset,
					toNotifyOnCheckpoint);
			case AT_LEAST_ONCE:
				return new CheckpointBarrierTracker(numberOfInputChannelsPerGate.sum(), toNotifyOnCheckpoint);
			default:
				throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + config.getCheckpointMode());
		}
	}

	static InputGate[] generateChannelIndexToInputGateMap(InputGate ...inputGates) {
		int numberOfInputChannels = Arrays.stream(inputGates).mapToInt(InputGate::getNumberOfInputChannels).sum();
		InputGate[] channelIndexToInputGate = new InputGate[numberOfInputChannels];
		int channelIndexOffset = 0;
		for (InputGate inputGate: inputGates) {
			for (int i = 0; i < inputGate.getNumberOfInputChannels(); ++i) {
				channelIndexToInputGate[channelIndexOffset + i] = inputGate;
			}
			channelIndexOffset += inputGate.getNumberOfInputChannels();
		}
		return channelIndexToInputGate;
	}

	static Map<InputGate, Integer> generateInputGateToChannelIndexOffsetMap(InputGate ...inputGates) {
		Map<InputGate, Integer> inputGateToChannelIndexOffset = new HashMap<>();
		int channelIndexOffset = 0;
		for (InputGate inputGate: inputGates) {
			inputGateToChannelIndexOffset.put(inputGate, channelIndexOffset);
			channelIndexOffset += inputGate.getNumberOfInputChannels();
		}
		return inputGateToChannelIndexOffset;
	}

	private static void registerCheckpointMetrics(TaskIOMetricGroup taskIOMetricGroup, CheckpointBarrierHandler barrierHandler) {
		taskIOMetricGroup.gauge(MetricNames.CHECKPOINT_ALIGNMENT_TIME, barrierHandler::getAlignmentDurationNanos);
		taskIOMetricGroup.gauge(MetricNames.CHECKPOINT_START_DELAY_TIME, barrierHandler::getCheckpointStartDelayNanos);
	}
}

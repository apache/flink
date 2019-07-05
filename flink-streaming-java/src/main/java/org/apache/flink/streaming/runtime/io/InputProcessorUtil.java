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
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility for creating {@link CheckpointedInputGate} based on checkpoint mode
 * for {@link StreamOneInputProcessor} and {@link StreamTwoInputProcessor}.
 */
@Internal
public class InputProcessorUtil {

	public static CheckpointedInputGate createCheckpointedInputGate(
			AbstractInvokable toNotifyOnCheckpoint,
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			InputGate inputGate,
			Configuration taskManagerConfig,
			String taskName) throws IOException {

		int pageSize = ConfigurationParserUtils.getPageSize(taskManagerConfig);

		BufferStorage bufferStorage = createBufferStorage(
			checkpointMode, ioManager, pageSize, taskManagerConfig, taskName);
		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			checkpointMode, inputGate.getNumberOfInputChannels(), taskName, toNotifyOnCheckpoint);
		return new CheckpointedInputGate(inputGate, bufferStorage, barrierHandler);
	}

	/**
	 * @return a pair of {@link CheckpointedInputGate} created for two corresponding
	 * {@link InputGate}s supplied as parameters.
	 */
	public static CheckpointedInputGate[] createCheckpointedInputGatePair(
			AbstractInvokable toNotifyOnCheckpoint,
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			InputGate inputGate1,
			InputGate inputGate2,
			Configuration taskManagerConfig,
			String taskName) throws IOException {

		int pageSize = ConfigurationParserUtils.getPageSize(taskManagerConfig);

		BufferStorage mainBufferStorage1 = createBufferStorage(
			checkpointMode, ioManager, pageSize, taskManagerConfig, taskName);
		BufferStorage mainBufferStorage2 = createBufferStorage(
			checkpointMode, ioManager, pageSize, taskManagerConfig, taskName);
		checkState(mainBufferStorage1.getMaxBufferedBytes() == mainBufferStorage2.getMaxBufferedBytes());

		BufferStorage linkedBufferStorage1 = new LinkedBufferStorage(
			mainBufferStorage1,
			mainBufferStorage2,
			mainBufferStorage1.getMaxBufferedBytes());
		BufferStorage linkedBufferStorage2 = new LinkedBufferStorage(
			mainBufferStorage2,
			mainBufferStorage1,
			mainBufferStorage1.getMaxBufferedBytes());

		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			checkpointMode,
			inputGate1.getNumberOfInputChannels() + inputGate2.getNumberOfInputChannels(),
			taskName,
			toNotifyOnCheckpoint);
		return new CheckpointedInputGate[] {
			new CheckpointedInputGate(inputGate1, linkedBufferStorage1, barrierHandler),
			new CheckpointedInputGate(inputGate2, linkedBufferStorage2, barrierHandler, inputGate1.getNumberOfInputChannels())
		};
	}

	private static CheckpointBarrierHandler createCheckpointBarrierHandler(
			CheckpointingMode checkpointMode,
			int numberOfInputChannels,
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint) {
		switch (checkpointMode) {
			case EXACTLY_ONCE:
				return new CheckpointBarrierAligner(
					numberOfInputChannels,
					taskName,
					toNotifyOnCheckpoint);
			case AT_LEAST_ONCE:
				return new CheckpointBarrierTracker(numberOfInputChannels, toNotifyOnCheckpoint);
			default:
				throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}
	}

	private static BufferStorage createBufferStorage(
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			int pageSize,
			Configuration taskManagerConfig,
			String taskName) throws IOException {
		switch (checkpointMode) {
			case EXACTLY_ONCE: {
				long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
				if (!(maxAlign == -1 || maxAlign > 0)) {
					throw new IllegalConfigurationException(
						TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
							+ " must be positive or -1 (infinite)");
				}

				if (taskManagerConfig.getBoolean(NettyShuffleEnvironmentOptions.NETWORK_CREDIT_MODEL)) {
					return new CachedBufferStorage(pageSize, maxAlign, taskName);
				} else {
					return new BufferSpiller(ioManager, pageSize, maxAlign, taskName);
				}
			}
			case AT_LEAST_ONCE:
				return new EmptyBufferStorage();
			default:
				throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}
	}
}

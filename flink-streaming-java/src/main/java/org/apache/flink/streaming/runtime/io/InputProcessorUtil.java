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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility for creating {@link CheckpointBarrierHandler} based on checkpoint mode
 * for {@link StreamInputProcessor} and {@link StreamTwoInputProcessor}.
 */
@Internal
public class InputProcessorUtil {

	public static SelectedReadingBarrierHandler createCheckpointBarrierHandler(
		boolean isCheckpointingEnabled,
		StreamTask<?, ?> checkpointedTask,
		CheckpointingMode checkpointMode,
		IOManager ioManager,
		Configuration taskManagerConfig,
		Collection<InputGate>... inputGateGroups) throws IOException {

		InputGate[][] inputGateGroupArray = new InputGate[inputGateGroups.length][];
		for (int i = 0; i < inputGateGroups.length; i++) {
			inputGateGroupArray[i] = inputGateGroups[i].toArray(new InputGate[0]);
		}

		return createCheckpointBarrierHandler(
			isCheckpointingEnabled,
			checkpointedTask,
			checkpointMode,
			ioManager,
			taskManagerConfig,
			inputGateGroupArray
		);
	}

	public static SelectedReadingBarrierHandler createCheckpointBarrierHandler(
		boolean isCheckpointingEnabled,
		StreamTask<?, ?> checkpointedTask,
		CheckpointingMode checkpointMode,
		IOManager ioManager,
		Configuration taskManagerConfig,
		InputGate[]... inputGateGroups) throws IOException {

		checkState(inputGateGroups.length > 0);

		SelectedReadingBarrierHandler barrierHandler;
		if (!isCheckpointingEnabled || checkpointMode == CheckpointingMode.AT_LEAST_ONCE) {
			InputGate[] unionInputGates = new InputGate[inputGateGroups.length];
			for (int i = 0; i < inputGateGroups.length; i++) {
				unionInputGates[i] = InputGateUtil.createInputGate(inputGateGroups[i]);
			}

			barrierHandler = new BarrierTracker(InputGateUtil.createInputGate(unionInputGates));
		} else if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
			long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
			if (!(maxAlign == -1 || maxAlign > 0)) {
				throw new IllegalConfigurationException(
					TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
						+ " must be positive or -1 (infinite)");
			}

			InputGate unionInputGate = InputGateUtil.createInputGate(inputGateGroups);
			if (taskManagerConfig.getBoolean(TaskManagerOptions.NETWORK_CREDIT_MODEL)) {
				barrierHandler = new BarrierBuffer(unionInputGate, new CachedBufferBlocker(unionInputGate.getPageSize()), maxAlign);
			} else {
				barrierHandler = new BarrierBuffer(unionInputGate, new BufferSpiller(ioManager, unionInputGate.getPageSize()), maxAlign);
			}
		} else {
			throw new IllegalArgumentException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}

		if (checkpointedTask != null) {
			barrierHandler.registerCheckpointEventHandler(checkpointedTask);
		}

		return barrierHandler;
	}
}

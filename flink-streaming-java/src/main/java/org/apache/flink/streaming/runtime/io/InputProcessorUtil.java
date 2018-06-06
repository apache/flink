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

/**
 * Utility for creating {@link CheckpointBarrierHandler} based on checkpoint mode
 * for {@link StreamInputProcessor} and {@link StreamTwoInputProcessor}.
 */
@Internal
public class InputProcessorUtil {

	public static CheckpointBarrierHandler createCheckpointBarrierHandler(
			StreamTask<?, ?> checkpointedTask,
			CheckpointingMode checkpointMode,
			IOManager ioManager,
			InputGate inputGate,
			Configuration taskManagerConfig) throws IOException {

		CheckpointBarrierHandler barrierHandler;
		if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
			long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
			if (!(maxAlign == -1 || maxAlign > 0)) {
				throw new IllegalConfigurationException(
					TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
					+ " must be positive or -1 (infinite)");
			}

			if (taskManagerConfig.getBoolean(TaskManagerOptions.NETWORK_CREDIT_MODEL)) {
				barrierHandler = new BarrierBuffer(inputGate, new CachedBufferBlocker(inputGate.getPageSize()), maxAlign);
			} else {
				barrierHandler = new BarrierBuffer(inputGate, new BufferSpiller(ioManager, inputGate.getPageSize()), maxAlign);
			}
		} else if (checkpointMode == CheckpointingMode.AT_LEAST_ONCE) {
			barrierHandler = new BarrierTracker(inputGate);
		} else {
			throw new IllegalArgumentException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}

		if (checkpointedTask != null) {
			barrierHandler.registerCheckpointEventHandler(checkpointedTask);
		}

		return barrierHandler;
	}
}

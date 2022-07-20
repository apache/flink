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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.recovery.RescalingStreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;

import java.util.function.Function;

/** Factory for {@link StreamTaskNetworkInput} and {@link RescalingStreamTaskNetworkInput}. */
public class StreamTaskNetworkInputFactory {
    /**
     * Factory method for {@link StreamTaskNetworkInput} or {@link RescalingStreamTaskNetworkInput}
     * depending on {@link InflightDataRescalingDescriptor}.
     */
    public static <T> StreamTaskInput<T> create(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            IOManager ioManager,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            InflightDataRescalingDescriptor rescalingDescriptorinflightDataRescalingDescriptor,
            Function<Integer, StreamPartitioner<?>> gatePartitioners,
            TaskInfo taskInfo) {
        return rescalingDescriptorinflightDataRescalingDescriptor.equals(
                        InflightDataRescalingDescriptor.NO_RESCALE)
                ? new StreamTaskNetworkInput<>(
                        checkpointedInputGate,
                        inputSerializer,
                        ioManager,
                        statusWatermarkValve,
                        inputIndex)
                : new RescalingStreamTaskNetworkInput<>(
                        checkpointedInputGate,
                        inputSerializer,
                        ioManager,
                        statusWatermarkValve,
                        inputIndex,
                        rescalingDescriptorinflightDataRescalingDescriptor,
                        gatePartitioners,
                        taskInfo);
    }
}

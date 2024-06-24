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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.groupingBy;

/**
 * Utility class for channel info serialization and conversions between single channel state handle
 * and merged state handle.
 */
public final class ChannelStateHelper {

    private ChannelStateHelper() {}

    public static final BiConsumerWithException<
                    ResultSubpartitionInfo, DataOutputStream, IOException>
            RESULT_SUBPARTITION_INFO_WRITER =
                    (info, out) -> {
                        out.writeInt(info.getPartitionIdx());
                        out.writeInt(info.getSubPartitionIdx());
                    };

    public static final BiConsumerWithException<InputChannelInfo, DataOutputStream, IOException>
            INPUT_CHANNEL_INFO_WRITER =
                    (info, dos) -> {
                        dos.writeInt(info.getGateIdx());
                        dos.writeInt(info.getInputChannelIdx());
                    };

    public static final FunctionWithException<DataInputStream, ResultSubpartitionInfo, IOException>
            RESULT_SUBPARTITION_INFO_READER =
                    is -> new ResultSubpartitionInfo(is.readInt(), is.readInt());

    public static final FunctionWithException<DataInputStream, InputChannelInfo, IOException>
            INPUT_CHANNEL_INFO_READER = is -> new InputChannelInfo(is.readInt(), is.readInt());

    public static Stream<StreamStateHandle> collectUniqueDisposableInChannelState(
            Stream<StateObjectCollection<? extends ChannelState>> stateCollections) {
        return stateCollections
                .flatMap(Collection::stream)
                .map(
                        handle -> {
                            if (handle instanceof AbstractChannelStateHandle) {
                                return ((AbstractChannelStateHandle) handle).getDelegate();

                            } else if (handle instanceof AbstractMergedChannelStateHandle) {
                                return ((AbstractMergedChannelStateHandle) handle).getDelegate();

                            } else {
                                throw new IllegalStateException(
                                        "Not Supported state handle : " + handle.getClass());
                            }
                        })
                .distinct();
    }

    public static StateObjectCollection<InputStateHandle> castToInputStateCollection(
            Collection<InputChannelStateHandle> handles) {
        if (handles == null || handles.size() == 0) {
            return StateObjectCollection.empty();
        }

        return new StateObjectCollection<>(
                handles.stream().map(e -> ((InputStateHandle) e)).collect(Collectors.toList()));
    }

    public static StateObjectCollection<OutputStateHandle> castToOutputStateCollection(
            Collection<ResultSubpartitionStateHandle> handles) {
        if (handles == null || handles.size() == 0) {
            return StateObjectCollection.empty();
        }

        return new StateObjectCollection<>(
                handles.stream().map(e -> ((OutputStateHandle) e)).collect(Collectors.toList()));
    }

    public static StateObjectCollection<InputStateHandle> mergeInputStateCollection(
            Collection<InputChannelStateHandle> handles) {
        if (handles == null || handles.size() == 0) {
            return StateObjectCollection.empty();
        }

        Collection<InputStateHandle> inputStateHandles =
                handles.stream()
                        .collect(groupingBy(e -> e.getDelegate()))
                        .values()
                        .stream()
                        .map(e -> MergedInputChannelStateHandle.fromChannelHandles(e))
                        .map(e -> ((InputStateHandle) e))
                        .collect(Collectors.toSet());
        return new StateObjectCollection<>(inputStateHandles);
    }

    public static StateObjectCollection<OutputStateHandle> mergeOutputStateCollection(
            Collection<ResultSubpartitionStateHandle> handles) {
        if (handles == null || handles.size() == 0) {
            return StateObjectCollection.empty();
        }

        Collection<OutputStateHandle> outputStateHandles =
                handles.stream()
                        .collect(groupingBy(e -> e.getDelegate()))
                        .values()
                        .stream()
                        .map(e -> MergedResultSubpartitionStateHandle.fromChannelHandles(e))
                        .map(e -> ((OutputStateHandle) e))
                        .collect(Collectors.toSet());
        return new StateObjectCollection<>(outputStateHandles);
    }

    public static StateObjectCollection<InputChannelStateHandle> extractUnmergedInputHandles(
            OperatorSubtaskState subtaskState) {
        List<InputChannelStateHandle> inputHandles =
                subtaskState.getInputChannelState().stream()
                        .flatMap(
                                h -> {
                                    if (h instanceof InputChannelStateHandle) {
                                        return singleton((InputChannelStateHandle) h).stream();
                                    } else if (h instanceof MergedInputChannelStateHandle) {
                                        return ((MergedInputChannelStateHandle) h)
                                                .getUnmergedHandles().stream();
                                    } else {
                                        throw new IllegalStateException(
                                                "Invalid input channel state : " + h.getClass());
                                    }
                                })
                        .collect(Collectors.toList());
        return inputHandles.size() > 0
                ? new StateObjectCollection<>(inputHandles)
                : StateObjectCollection.empty();
    }

    public static StateObjectCollection<ResultSubpartitionStateHandle> extractUnmergedOutputHandles(
            OperatorSubtaskState subtaskState) {
        List<ResultSubpartitionStateHandle> outputHandles =
                subtaskState.getResultSubpartitionState().stream()
                        .flatMap(
                                h -> {
                                    if (h instanceof ResultSubpartitionStateHandle) {
                                        return singleton((ResultSubpartitionStateHandle) h)
                                                .stream();
                                    } else if (h instanceof MergedResultSubpartitionStateHandle) {
                                        return ((MergedResultSubpartitionStateHandle) h)
                                                .getUnmergedHandles().stream();
                                    } else {
                                        throw new IllegalStateException(
                                                "Invalid output channel state : " + h.getClass());
                                    }
                                })
                        .collect(Collectors.toList());
        return outputHandles.size() > 0
                ? new StateObjectCollection<>(outputHandles)
                : StateObjectCollection.empty();
    }
}

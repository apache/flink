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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.InputStateHandle;
import org.apache.flink.runtime.state.MergedInputChannelStateHandle;
import org.apache.flink.runtime.state.MergedResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.OutputStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A collection of utility methods for testing about channel state. */
public class ChannelStateTestUtils {
    private static final int SUBTASK_INDEX_UPPER = 10000;

    public static InputChannelStateHandle randomInputChannelStateHandle() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StreamStateHandle delegated = randomDelegatedHandle();

        return new InputChannelStateHandle(
                random.nextInt(0, SUBTASK_INDEX_UPPER),
                new InputChannelInfo(
                        random.nextInt(0, SUBTASK_INDEX_UPPER),
                        random.nextInt(0, SUBTASK_INDEX_UPPER)),
                delegated,
                randomOffsets(),
                delegated.getStateSize());
    }

    public static ResultSubpartitionStateHandle randomResultSubpartitionStateHandle() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StreamStateHandle delegated = randomDelegatedHandle();

        return new ResultSubpartitionStateHandle(
                random.nextInt(0, SUBTASK_INDEX_UPPER),
                new ResultSubpartitionInfo(
                        random.nextInt(0, SUBTASK_INDEX_UPPER),
                        random.nextInt(0, SUBTASK_INDEX_UPPER)),
                delegated,
                randomOffsets(),
                delegated.getStateSize());
    }

    public static List<InputChannelStateHandle> randomInputChannelStateHandlesFromSameSubtask() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int subtaskIndex = random.nextInt(0, SUBTASK_INDEX_UPPER);
        StreamStateHandle delegated = randomDelegatedHandle();

        int channelHandleCount = random.nextInt(20, 50);
        List<InputChannelStateHandle> inputHandles = new ArrayList<>(channelHandleCount);
        for (int i = 0; i < channelHandleCount; i++) {
            inputHandles.add(
                    new InputChannelStateHandle(
                            subtaskIndex,
                            new InputChannelInfo(
                                    random.nextInt(0, SUBTASK_INDEX_UPPER),
                                    random.nextInt(0, SUBTASK_INDEX_UPPER)),
                            delegated,
                            randomOffsets(),
                            delegated.getStateSize()));
        }

        return inputHandles;
    }

    public static MergedInputChannelStateHandle randomMergedInputChannelStateHandle() {
        return MergedInputChannelStateHandle.fromChannelHandles(
                randomInputChannelStateHandlesFromSameSubtask());
    }

    public static List<ResultSubpartitionStateHandle>
            randomResultSubpartitionStateHandlesFromSameSubtask() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int subtaskIndex = random.nextInt(0, SUBTASK_INDEX_UPPER);
        StreamStateHandle delegated = randomDelegatedHandle();

        int channelHandleCount = random.nextInt(20, 50);
        List<ResultSubpartitionStateHandle> outputHandles = new ArrayList<>(channelHandleCount);
        for (int i = 0; i < channelHandleCount; i++) {
            outputHandles.add(
                    new ResultSubpartitionStateHandle(
                            subtaskIndex,
                            new ResultSubpartitionInfo(
                                    random.nextInt(0, SUBTASK_INDEX_UPPER),
                                    random.nextInt(0, SUBTASK_INDEX_UPPER)),
                            delegated,
                            randomOffsets(),
                            delegated.getStateSize()));
        }

        return outputHandles;
    }

    public static MergedResultSubpartitionStateHandle randomMergedResultSubpartitionStateHandle() {
        return MergedResultSubpartitionStateHandle.fromChannelHandles(
                randomResultSubpartitionStateHandlesFromSameSubtask());
    }

    private static StreamStateHandle randomDelegatedHandle() {
        byte[] randomBytes = String.valueOf(ThreadLocalRandom.current().nextLong()).getBytes();
        return new ByteStreamStateHandle("delegated", randomBytes);
    }

    private static List<Long> randomOffsets() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int offsetSize = random.nextInt(10, 20);
        List<Long> offsets = new ArrayList<>(offsetSize);
        for (int i = 0; i < offsetSize; i++) {
            offsets.add(random.nextLong());
        }
        return offsets;
    }

    static void testSerializeInputHandle(
            ChannelStateHandleSerializer serializer, Supplier<InputStateHandle> handleGenerator)
            throws IOException {
        InputStateHandle inputHandle = handleGenerator.get();

        ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(inMemBuffer);
        serializer.serialize(inputHandle, outputStream);

        DataInputStream inputStream =
                new DataInputStream(new ByteArrayInputStream(inMemBuffer.toByteArray()));
        assertEquals(inputHandle, serializer.deserializeInputStateHandle(inputStream, null));
    }

    static void testSerializeOutputHandle(
            ChannelStateHandleSerializer serializer, Supplier<OutputStateHandle> handleGenerator)
            throws IOException {
        OutputStateHandle outputHandle = handleGenerator.get();

        ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(inMemBuffer);
        serializer.serialize(outputHandle, outputStream);

        DataInputStream inputStream =
                new DataInputStream(new ByteArrayInputStream(inMemBuffer.toByteArray()));
        assertEquals(outputHandle, serializer.deserializeOutputStateHandle(inputStream, null));
    }
}

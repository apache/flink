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

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChannelStateHandleSerializerV1Test {

    @Test
    void testSerializeInputHandle() throws IOException {
        ChannelStateHandleSerializerV1 serializer = new ChannelStateHandleSerializerV1();

        Tuple5<Integer, Integer, Integer, StreamStateHandle, List<Long>> handleFields =
                generateRandomChannelHandleFields();

        InputChannelStateHandle inputHandle =
                new InputChannelStateHandle(
                        handleFields.f0,
                        new InputChannelInfo(handleFields.f1, handleFields.f2),
                        handleFields.f3,
                        handleFields.f4,
                        handleFields.f3.getStateSize());

        ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(inMemBuffer);
        serializer.serialize(inputHandle, outputStream);

        DataInputStream inputStream =
                new DataInputStream(new ByteArrayInputStream(inMemBuffer.toByteArray()));
        assertEquals(inputHandle, serializer.deserializeInputStateHandle(inputStream, null));
    }

    @Test
    void testSerializeOutputHandle() throws IOException {
        ChannelStateHandleSerializerV1 serializer = new ChannelStateHandleSerializerV1();

        Tuple5<Integer, Integer, Integer, StreamStateHandle, List<Long>> handleFields =
                generateRandomChannelHandleFields();

        ResultSubpartitionStateHandle outputHandle =
                new ResultSubpartitionStateHandle(
                        handleFields.f0,
                        new ResultSubpartitionInfo(handleFields.f1, handleFields.f2),
                        handleFields.f3,
                        handleFields.f4,
                        handleFields.f3.getStateSize());

        ByteArrayOutputStream inMemBuffer = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(inMemBuffer);
        serializer.serialize(outputHandle, outputStream);

        DataInputStream inputStream =
                new DataInputStream(new ByteArrayInputStream(inMemBuffer.toByteArray()));
        assertEquals(outputHandle, serializer.deserializeInputStateHandle(inputStream, null));
    }

    private Tuple5<Integer, Integer, Integer, StreamStateHandle, List<Long>>
            generateRandomChannelHandleFields() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int subtaskIndex = random.nextInt();
        int infoIdx1 = random.nextInt();
        int infoIdx2 = random.nextInt();

        byte[] randomBytes = String.valueOf(random.nextLong()).getBytes();
        StreamStateHandle delegate = new ByteStreamStateHandle("delegated", randomBytes);

        int offsetSize = random.nextInt(10, 20);
        List<Long> offsets = new ArrayList<>(offsetSize);
        for (int i = 0; i < offsetSize; i++) {
            offsets.add(random.nextLong());
        }

        return Tuple5.of(subtaskIndex, infoIdx1, infoIdx2, delegate, offsets);
    }
}

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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.AbstractMergedChannelStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.InputStateHandle;
import org.apache.flink.runtime.state.MergedInputChannelStateHandle;
import org.apache.flink.runtime.state.MergedResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.OutputStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.flink.runtime.checkpoint.metadata.ChannelStateHandleSerializerV1.deserializeChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.metadata.ChannelStateHandleSerializerV1.serializeChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.metadata.MetadataV2V3SerializerBase.deserializeStreamStateHandle;
import static org.apache.flink.runtime.checkpoint.metadata.MetadataV2V3SerializerBase.serializeStreamStateHandle;
import static org.apache.flink.runtime.state.ChannelStateHelper.INPUT_CHANNEL_INFO_READER;
import static org.apache.flink.runtime.state.ChannelStateHelper.INPUT_CHANNEL_INFO_WRITER;
import static org.apache.flink.runtime.state.ChannelStateHelper.RESULT_SUBPARTITION_INFO_READER;
import static org.apache.flink.runtime.state.ChannelStateHelper.RESULT_SUBPARTITION_INFO_WRITER;

/**
 * ChannelStateHandlerSerializer support extending handle types, first used in {@link
 * MetadataV5Serializer} .
 */
class ChannelStateHandleSerializerV2 implements ChannelStateHandleSerializer {

    private static final byte INPUT_CHANNEL_STATE_HANDLE = 1;
    private static final byte RESULT_SUBPARTITION_STATE_HANDLE = 2;
    private static final byte MERGED_INPUT_CHANNEL_STATE_HANDLE = 3;
    private static final byte MERGED_RESULT_SUBPARTITION_STATE_HANDLE = 4;

    @Override
    public void serialize(OutputStateHandle handle, DataOutputStream dos) throws IOException {
        if (handle instanceof ResultSubpartitionStateHandle) {
            dos.writeByte(RESULT_SUBPARTITION_STATE_HANDLE);

            serializeChannelStateHandle(
                    (ResultSubpartitionStateHandle) handle, dos, RESULT_SUBPARTITION_INFO_WRITER);
        } else if (handle instanceof MergedResultSubpartitionStateHandle) {
            dos.writeByte(MERGED_RESULT_SUBPARTITION_STATE_HANDLE);

            serializeMergedChannelStateHandle(
                    (AbstractMergedChannelStateHandle<
                                    ResultSubpartitionInfo,
                                    AbstractChannelStateHandle<ResultSubpartitionInfo>>)
                            handle,
                    dos);
        } else {
            throw new IllegalStateException("Unsupported input state handle.");
        }
    }

    @Override
    public OutputStateHandle deserializeOutputStateHandle(
            DataInputStream dis, MetadataV2V3SerializerBase.DeserializationContext context)
            throws IOException {

        final int type = dis.read();

        if (RESULT_SUBPARTITION_STATE_HANDLE == type) {
            return deserializeChannelStateHandle(
                    RESULT_SUBPARTITION_INFO_READER,
                    ResultSubpartitionStateHandle::new,
                    dis,
                    context);

        } else if (MERGED_RESULT_SUBPARTITION_STATE_HANDLE == type) {
            Tuple4<Integer, StreamStateHandle, Long, byte[]> t4 =
                    deserializeMergedChannelStateHandle(dis, context);
            return new MergedResultSubpartitionStateHandle(t4.f0, t4.f1, t4.f2, t4.f3);
        } else {
            throw new IllegalStateException("Unknown output state handle type.");
        }
    }

    @Override
    public void serialize(InputStateHandle handle, DataOutputStream dos) throws IOException {
        if (handle instanceof InputChannelStateHandle) {
            dos.writeByte(INPUT_CHANNEL_STATE_HANDLE);

            serializeChannelStateHandle(
                    (InputChannelStateHandle) handle, dos, INPUT_CHANNEL_INFO_WRITER);
        } else if (handle instanceof MergedInputChannelStateHandle) {
            dos.writeByte(MERGED_INPUT_CHANNEL_STATE_HANDLE);

            serializeMergedChannelStateHandle(
                    (AbstractMergedChannelStateHandle<
                                    InputChannelInfo, AbstractChannelStateHandle<InputChannelInfo>>)
                            handle,
                    dos);
        } else {
            throw new IllegalStateException("Unsupported output state handle.");
        }
    }

    @Override
    public InputStateHandle deserializeInputStateHandle(
            DataInputStream dis, MetadataV2V3SerializerBase.DeserializationContext context)
            throws IOException {

        final int type = dis.read();

        if (INPUT_CHANNEL_STATE_HANDLE == type) {
            return deserializeChannelStateHandle(
                    INPUT_CHANNEL_INFO_READER, InputChannelStateHandle::new, dis, context);

        } else if (MERGED_INPUT_CHANNEL_STATE_HANDLE == type) {
            Tuple4<Integer, StreamStateHandle, Long, byte[]> t4 =
                    deserializeMergedChannelStateHandle(dis, context);
            return new MergedInputChannelStateHandle(t4.f0, t4.f1, t4.f2, t4.f3);

        } else {
            throw new IllegalStateException("Unknown input state handle type.");
        }
    }

    private <Info> void serializeMergedChannelStateHandle(
            AbstractMergedChannelStateHandle<Info, AbstractChannelStateHandle<Info>> handle,
            DataOutputStream dos)
            throws IOException {

        dos.writeInt(handle.getSubtaskIndex());
        dos.writeLong(handle.getStateSize());
        serializeStreamStateHandle(handle.getDelegate(), dos);

        dos.writeInt(handle.getSerializedChannelOffsets().length);
        dos.write(handle.getSerializedChannelOffsets());
    }

    private Tuple4<Integer, StreamStateHandle, Long, byte[]> deserializeMergedChannelStateHandle(
            DataInputStream dis, MetadataV2V3SerializerBase.DeserializationContext context)
            throws IOException {

        final int subtaskIndex = dis.readInt();
        final long stateSize = dis.readLong();
        final StreamStateHandle delegateHandle = deserializeStreamStateHandle(dis, context);

        final int serializedChannelOffsetsLength = dis.readInt();
        final byte[] serializedChannelOffsets = new byte[serializedChannelOffsetsLength];
        dis.readFully(serializedChannelOffsets);

        return Tuple4.of(subtaskIndex, delegateHandle, stateSize, serializedChannelOffsets);
    }
}

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

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.runtime.state.ChannelStateHelper.INPUT_CHANNEL_INFO_READER;
import static org.apache.flink.runtime.state.ChannelStateHelper.INPUT_CHANNEL_INFO_WRITER;

public class MergedInputChannelStateHandle
        extends AbstractMergedChannelStateHandle<InputChannelInfo, InputChannelStateHandle>
        implements InputStateHandle {

    private static final long serialVersionUID = 1L;

    public MergedInputChannelStateHandle(
            int subtaskIndex,
            StreamStateHandle delegate,
            long size,
            byte[] serializedChannelOffsets) {
        super(subtaskIndex, delegate, size, serializedChannelOffsets);
    }

    @Override
    protected void writeInfo(InputChannelInfo inputChannelInfo, DataOutputStream dos)
            throws IOException {
        INPUT_CHANNEL_INFO_WRITER.accept(inputChannelInfo, dos);
    }

    @Override
    protected InputChannelInfo readInfo(DataInputStream dis) throws IOException {
        return INPUT_CHANNEL_INFO_READER.apply(dis);
    }

    @Override
    protected InputChannelStateHandle createUnmergedHandle(
            int subtaskIndex,
            StreamStateHandle delegate,
            InputChannelInfo info,
            long stateSize,
            List<Long> offsets) {
        return new InputChannelStateHandle(subtaskIndex, info, delegate, offsets, stateSize);
    }

    private MergedInputChannelStateHandle(Collection<InputChannelStateHandle> handles) {
        super(handles);
    }

    public static MergedInputChannelStateHandle fromChannelHandles(
            Collection<InputChannelStateHandle> handles) {
        return new MergedInputChannelStateHandle(handles);
    }
}

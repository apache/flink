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

import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.runtime.state.ChannelStateHelper.RESULT_SUBPARTITION_INFO_READER;
import static org.apache.flink.runtime.state.ChannelStateHelper.RESULT_SUBPARTITION_INFO_WRITER;

public class MergedResultSubpartitionStateHandle
        extends AbstractMergedChannelStateHandle<
                ResultSubpartitionInfo, ResultSubpartitionStateHandle>
        implements OutputStateHandle {

    private static final long serialVersionUID = 1L;

    public MergedResultSubpartitionStateHandle(
            int subtaskIndex,
            StreamStateHandle delegate,
            long size,
            byte[] serializedChannelOffsets) {
        super(subtaskIndex, delegate, size, serializedChannelOffsets);
    }

    @Override
    protected void writeInfo(ResultSubpartitionInfo resultSubpartitionInfo, DataOutputStream dos)
            throws IOException {
        RESULT_SUBPARTITION_INFO_WRITER.accept(resultSubpartitionInfo, dos);
    }

    @Override
    protected ResultSubpartitionInfo readInfo(DataInputStream dis) throws IOException {
        return RESULT_SUBPARTITION_INFO_READER.apply(dis);
    }

    @Override
    protected ResultSubpartitionStateHandle createUnmergedHandle(
            int subtaskIndex,
            StreamStateHandle delegate,
            ResultSubpartitionInfo info,
            long stateSize,
            List<Long> offsets) {
        return new ResultSubpartitionStateHandle(subtaskIndex, info, delegate, offsets, stateSize);
    }

    private MergedResultSubpartitionStateHandle(Collection<ResultSubpartitionStateHandle> handles) {
        super(handles);
    }

    public static MergedResultSubpartitionStateHandle fromChannelHandles(
            Collection<ResultSubpartitionStateHandle> handles) {
        return new MergedResultSubpartitionStateHandle(handles);
    }
}

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

package org.apache.flink.streaming.api.runners.python.beam.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/** BeamBagStateHandler handles operations on {@link ListState}, which backs Beam bag states. */
public class BeamBagStateHandler extends AbstractBeamStateHandler<ListState<byte[]>> {

    private static final String MERGE_NAMESPACES_MARK = "merge_namespaces";

    @Nullable private final TypeSerializer<?> namespaceSerializer;

    /** Reusable InputStream used to holding the elements to be deserialized. */
    private final ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    private final DataInputViewStreamWrapper baisWrapper;

    public BeamBagStateHandler(@Nullable TypeSerializer<?> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        this.bais = new ByteArrayInputStreamWithPos();
        this.baisWrapper = new DataInputViewStreamWrapper(bais);
    }

    public BeamFnApi.StateResponse.Builder handleGet(
            BeamFnApi.StateRequest request, ListState<byte[]> listState) throws Exception {
        List<ByteString> byteStrings = convertToByteString(listState);

        return BeamFnApi.StateResponse.newBuilder()
                .setId(request.getId())
                .setGet(
                        BeamFnApi.StateGetResponse.newBuilder()
                                .setData(ByteString.copyFrom(byteStrings)));
    }

    public BeamFnApi.StateResponse.Builder handleAppend(
            BeamFnApi.StateRequest request, ListState<byte[]> listState) throws Exception {
        if (request.getStateKey()
                .getBagUserState()
                .getTransformId()
                .equals(MERGE_NAMESPACES_MARK)) {
            Preconditions.checkNotNull(namespaceSerializer);
            // get namespaces to merge
            byte[] namespacesBytes = request.getAppend().getData().toByteArray();
            bais.setBuffer(namespacesBytes, 0, namespacesBytes.length);
            int namespaceCount = baisWrapper.readInt();
            Set<Object> namespaces = new HashSet<>();
            for (int i = 0; i < namespaceCount; i++) {
                namespaces.add(namespaceSerializer.deserialize(baisWrapper));
            }
            byte[] targetNamespaceByte =
                    request.getStateKey().getBagUserState().getWindow().toByteArray();
            bais.setBuffer(targetNamespaceByte, 0, targetNamespaceByte.length);
            Object targetNamespace = namespaceSerializer.deserialize(baisWrapper);
            ((InternalListState<Object, Object, byte[]>) listState)
                    .mergeNamespaces(targetNamespace, namespaces);
        } else {
            // get values
            byte[] valueBytes = request.getAppend().getData().toByteArray();
            listState.add(valueBytes);
        }
        return BeamFnApi.StateResponse.newBuilder()
                .setId(request.getId())
                .setAppend(BeamFnApi.StateAppendResponse.getDefaultInstance());
    }

    public BeamFnApi.StateResponse.Builder handleClear(
            BeamFnApi.StateRequest request, ListState<byte[]> listState) throws Exception {
        listState.clear();

        return BeamFnApi.StateResponse.newBuilder()
                .setId(request.getId())
                .setClear(BeamFnApi.StateClearResponse.getDefaultInstance());
    }

    private static List<ByteString> convertToByteString(ListState<byte[]> listState)
            throws Exception {
        List<ByteString> ret = new LinkedList<>();
        if (listState.get() == null) {
            return ret;
        }
        for (byte[] v : listState.get()) {
            ret.add(ByteString.copyFrom(v));
        }
        return ret;
    }
}

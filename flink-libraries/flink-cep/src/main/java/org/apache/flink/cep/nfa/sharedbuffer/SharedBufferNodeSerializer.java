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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Serializer for {@link SharedBufferNode}. */
public class SharedBufferNodeSerializer extends TypeSerializerSingleton<SharedBufferNode> {

    private static final long serialVersionUID = -6687780732295439832L;

    private final TypeSerializer<SharedBufferEdge> edgeSerializer;

    public SharedBufferNodeSerializer() {
        this.edgeSerializer = new SharedBufferEdge.SharedBufferEdgeSerializer();
    }

    SharedBufferNodeSerializer(TypeSerializer<SharedBufferEdge> edgeSerializer) {
        this.edgeSerializer = checkNotNull(edgeSerializer);
    }

    public TypeSerializer<SharedBufferEdge> getEdgeSerializer() {
        return edgeSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public SharedBufferNode createInstance() {
        return new SharedBufferNode(new ArrayList<>());
    }

    @Override
    public SharedBufferNode copy(SharedBufferNode from) {
        return new SharedBufferNode(
                from.getEdges().stream()
                        .map(
                                edge ->
                                        new Lockable<>(
                                                edgeSerializer.copy(edge.getElement()),
                                                edge.getRefCounter()))
                        .collect(Collectors.toList()));
    }

    @Override
    public SharedBufferNode copy(SharedBufferNode from, SharedBufferNode reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(SharedBufferNode record, DataOutputView target) throws IOException {
        List<Lockable<SharedBufferEdge>> edges = record.getEdges();
        target.writeInt(edges.size());
        for (Lockable<SharedBufferEdge> edge : edges) {
            target.writeInt(edge.getRefCounter());
            edgeSerializer.serialize(edge.getElement(), target);
        }
    }

    @Override
    public SharedBufferNode deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        List<Lockable<SharedBufferEdge>> edges = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            int refCount = source.readInt();
            SharedBufferEdge edge = edgeSerializer.deserialize(source);
            edges.add(new Lockable<>(edge, refCount));
        }
        return new SharedBufferNode(edges);
    }

    @Override
    public SharedBufferNode deserialize(SharedBufferNode reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        for (int i = 0; i < length; i++) {
            target.writeInt(source.readInt());
            edgeSerializer.copy(source, target);
        }
    }

    @Override
    public TypeSerializerSnapshot<SharedBufferNode> snapshotConfiguration() {
        return new SharedBufferNodeSerializerSnapshotV2(this);
    }
}

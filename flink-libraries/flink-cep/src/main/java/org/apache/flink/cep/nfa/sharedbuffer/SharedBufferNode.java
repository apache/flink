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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferEdge.SharedBufferEdgeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyedStateBackend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An entry in {@link SharedBuffer} that allows to store relations between different entries. */
public class SharedBufferNode {

    private final List<Lockable<SharedBufferEdge>> edges;

    public SharedBufferNode() {
        edges = new ArrayList<>();
    }

    SharedBufferNode(List<Lockable<SharedBufferEdge>> edges) {
        this.edges = edges;
    }

    public List<Lockable<SharedBufferEdge>> getEdges() {
        return edges;
    }

    public void addEdge(SharedBufferEdge edge) {
        edges.add(new Lockable<>(edge, 0));
    }

    @Override
    public String toString() {
        return "SharedBufferNode{" + "edges=" + edges + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SharedBufferNode that = (SharedBufferNode) o;
        return Objects.equals(edges, that.edges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edges);
    }

    /**
     * Serializer for {@link SharedBufferNode}.
     *
     * <p>This serializer had to be deprecated and you cannot directly migrate to the newer version.
     * The new structure requires additional information from other nodes. The migration happens in
     * {@link SharedBuffer#migrateOldState(KeyedStateBackend, ValueState)}.
     *
     * @deprecated was used in <= 1.12, use {@link
     *     org.apache.flink.cep.nfa.sharedbuffer.SharedBufferNodeSerializer} instead.
     */
    @Deprecated
    public static class SharedBufferNodeSerializer
            extends TypeSerializerSingleton<SharedBufferNode> {

        private static final long serialVersionUID = -6687780732295439832L;

        private final ListSerializer<SharedBufferEdge> edgesSerializer;

        public SharedBufferNodeSerializer() {
            this.edgesSerializer = new ListSerializer<>(new SharedBufferEdgeSerializer());
        }

        private SharedBufferNodeSerializer(ListSerializer<SharedBufferEdge> edgesSerializer) {
            this.edgesSerializer = checkNotNull(edgesSerializer);
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
            throw new UnsupportedOperationException("Should not be used");
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
            throw new UnsupportedOperationException("We should no longer use it for serialization");
        }

        @Override
        public SharedBufferNode deserialize(DataInputView source) throws IOException {
            List<SharedBufferEdge> edges = edgesSerializer.deserialize(source);
            SharedBufferNode node = new SharedBufferNode();
            for (SharedBufferEdge edge : edges) {
                node.addEdge(edge);
            }
            return node;
        }

        @Override
        public SharedBufferNode deserialize(SharedBufferNode reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            edgesSerializer.copy(source, target);
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<SharedBufferNode> snapshotConfiguration() {
            return new SharedBufferNodeSerializerSnapshot(this);
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class SharedBufferNodeSerializerSnapshot
                extends CompositeTypeSerializerSnapshot<
                        SharedBufferNode, SharedBufferNodeSerializer> {

            private static final int VERSION = 1;

            public SharedBufferNodeSerializerSnapshot() {
                super(SharedBufferNodeSerializer.class);
            }

            public SharedBufferNodeSerializerSnapshot(
                    SharedBufferNodeSerializer sharedBufferNodeSerializer) {
                super(sharedBufferNodeSerializer);
            }

            @Override
            protected int getCurrentOuterSnapshotVersion() {
                return VERSION;
            }

            @Override
            @SuppressWarnings("unchecked")
            protected SharedBufferNodeSerializer createOuterSerializerWithNestedSerializers(
                    TypeSerializer<?>[] nestedSerializers) {
                return new SharedBufferNodeSerializer(
                        (ListSerializer<SharedBufferEdge>) nestedSerializers[0]);
            }

            @Override
            protected TypeSerializer<?>[] getNestedSerializers(
                    SharedBufferNodeSerializer outerSerializer) {
                return new TypeSerializer<?>[] {outerSerializer.edgesSerializer};
            }
        }
    }
}

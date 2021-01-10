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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Versioned edge in {@link SharedBuffer} that allows retrieving predecessors. */
public class SharedBufferEdge {

    private final NodeId target;
    private final DeweyNumber deweyNumber;

    /**
     * Creates versioned (with {@link DeweyNumber}) edge that points to the target entry.
     *
     * @param target id of target entry
     * @param deweyNumber version for this edge
     */
    public SharedBufferEdge(NodeId target, DeweyNumber deweyNumber) {
        this.target = target;
        this.deweyNumber = deweyNumber;
    }

    NodeId getTarget() {
        return target;
    }

    DeweyNumber getDeweyNumber() {
        return deweyNumber;
    }

    @Override
    public String toString() {
        return "SharedBufferEdge{" + "target=" + target + ", deweyNumber=" + deweyNumber + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SharedBufferEdge that = (SharedBufferEdge) o;
        return Objects.equals(target, that.target) && Objects.equals(deweyNumber, that.deweyNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, deweyNumber);
    }

    /** Serializer for {@link SharedBufferEdge}. */
    public static class SharedBufferEdgeSerializer
            extends TypeSerializerSingleton<SharedBufferEdge> {

        private static final long serialVersionUID = -5122474955050663979L;

        /**
         * NOTE: these serializer fields should actually be final. The reason that it isn't final is
         * due to backward compatible deserialization paths. See {@link
         * #readObject(ObjectInputStream)}.
         */
        private TypeSerializer<NodeId> nodeIdSerializer;

        private TypeSerializer<DeweyNumber> deweyNumberSerializer;

        public SharedBufferEdgeSerializer() {
            this(new NodeId.NodeIdSerializer(), DeweyNumber.DeweyNumberSerializer.INSTANCE);
        }

        private SharedBufferEdgeSerializer(
                TypeSerializer<NodeId> nodeIdSerializer,
                TypeSerializer<DeweyNumber> deweyNumberSerializer) {
            this.nodeIdSerializer = checkNotNull(nodeIdSerializer);
            this.deweyNumberSerializer = checkNotNull(deweyNumberSerializer);
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public SharedBufferEdge createInstance() {
            return null;
        }

        @Override
        public SharedBufferEdge copy(SharedBufferEdge from) {
            return new SharedBufferEdge(from.target, from.deweyNumber);
        }

        @Override
        public SharedBufferEdge copy(SharedBufferEdge from, SharedBufferEdge reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(SharedBufferEdge record, DataOutputView target) throws IOException {
            nodeIdSerializer.serialize(record.target, target);
            deweyNumberSerializer.serialize(record.deweyNumber, target);
        }

        @Override
        public SharedBufferEdge deserialize(DataInputView source) throws IOException {
            NodeId target = nodeIdSerializer.deserialize(source);
            DeweyNumber deweyNumber = deweyNumberSerializer.deserialize(source);
            return new SharedBufferEdge(target, deweyNumber);
        }

        @Override
        public SharedBufferEdge deserialize(SharedBufferEdge reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            nodeIdSerializer.copy(source, target);
            deweyNumberSerializer.copy(source, target);
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<SharedBufferEdge> snapshotConfiguration() {
            return new SharedBufferEdgeSerializerSnapshot(this);
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class SharedBufferEdgeSerializerSnapshot
                extends CompositeTypeSerializerSnapshot<
                        SharedBufferEdge, SharedBufferEdgeSerializer> {

            private static final int VERSION = 1;

            public SharedBufferEdgeSerializerSnapshot() {
                super(SharedBufferEdgeSerializer.class);
            }

            public SharedBufferEdgeSerializerSnapshot(
                    SharedBufferEdgeSerializer sharedBufferEdgeSerializer) {
                super(sharedBufferEdgeSerializer);
            }

            @Override
            protected int getCurrentOuterSnapshotVersion() {
                return VERSION;
            }

            @Override
            protected SharedBufferEdgeSerializer createOuterSerializerWithNestedSerializers(
                    TypeSerializer<?>[] nestedSerializers) {
                return new SharedBufferEdgeSerializer(
                        (NodeId.NodeIdSerializer) nestedSerializers[0],
                        (DeweyNumber.DeweyNumberSerializer) nestedSerializers[1]);
            }

            @Override
            protected TypeSerializer<?>[] getNestedSerializers(
                    SharedBufferEdgeSerializer outerSerializer) {
                return new TypeSerializer<?>[] {
                    outerSerializer.nodeIdSerializer, outerSerializer.deweyNumberSerializer
                };
            }
        }

        // ------------------------------------------------------------------------

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();

            if (nodeIdSerializer == null) {
                // the nested serializers will be null if this was read from a savepoint taken with
                // versions
                // lower than Flink 1.7; in this case, we explicitly create instances for the nested
                // serializers
                this.nodeIdSerializer = new NodeId.NodeIdSerializer();
                this.deweyNumberSerializer = DeweyNumber.DeweyNumberSerializer.INSTANCE;
            }
        }
    }
}

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

package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

/** {@link TypeSerializer} for {@link Node}. */
@SuppressWarnings("NullableProblems")
public class NodeSerializer extends CompositeSerializer<Node> {

    private static final LongSerializer LONG_SERIALIZER = LongSerializer.INSTANCE;
    private static final TypeSerializer<?> NULLABLE_LONG_SERIALIZER =
            NullableSerializer.wrap(LONG_SERIALIZER, true);

    public NodeSerializer(TypeSerializer<RowData> serializer) {
        this(null, NodeField.getFieldSerializers(serializer));
    }

    protected NodeSerializer(
            PrecomputedParameters precomputed, TypeSerializer<?>[] originalSerializers) {
        //noinspection unchecked
        super(
                PrecomputedParameters.precompute(
                        true, true, (TypeSerializer<Object>[]) originalSerializers),
                originalSerializers);
    }

    private NodeSerializer(TypeSerializer<?>[] nestedSerializers) {
        this(null, nestedSerializers);
    }

    @Override
    public Node createInstance(Object... values) {
        return new Node(
                NodeField.ROW.get(values),
                NodeField.SEQ_NO.get(values),
                NodeField.PREV_SEQ_NO.get(values),
                NodeField.NEXT_SEQ_NO.get(values),
                NodeField.NEXT_SEQ_NO_FOR_RECORD.get(values),
                NodeField.TIMESTAMP.get(values));
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    protected Object getField(Node node, int index) {
        return NodeField.get(node, index);
    }

    @Override
    protected CompositeSerializer<Node> createSerializerInstance(
            PrecomputedParameters precomputed, TypeSerializer<?>... originalSerializers) {
        return new NodeSerializer(precomputed, originalSerializers);
    }

    @Override
    public TypeSerializerSnapshot<Node> snapshotConfiguration() {
        return new NodeSerializerSnapshot(this);
    }

    @Override
    protected void setField(Node value, int index, Object fieldValue) {
        throw new UnsupportedOperationException();
    }

    private enum NodeField {
        ROW {
            @Override
            Object get(Node node) {
                return node.getRow();
            }

            @Override
            public TypeSerializer<?> getSerializer(TypeSerializer<RowData> serializer) {
                return serializer;
            }
        },
        SEQ_NO {
            @Override
            Object get(Node node) {
                return node.getSqn();
            }

            @Override
            public TypeSerializer<?> getSerializer(TypeSerializer<RowData> serializer) {
                return LONG_SERIALIZER;
            }
        },
        PREV_SEQ_NO {
            @Override
            Object get(Node node) {
                return node.getPrevSqn();
            }

            @Override
            public TypeSerializer<?> getSerializer(TypeSerializer<RowData> serializer) {
                return NULLABLE_LONG_SERIALIZER;
            }
        },
        NEXT_SEQ_NO {
            @Override
            Object get(Node node) {
                return node.getNextSqn();
            }

            @Override
            public TypeSerializer<?> getSerializer(TypeSerializer<RowData> serializer) {
                return NULLABLE_LONG_SERIALIZER;
            }
        },
        NEXT_SEQ_NO_FOR_RECORD {
            @Override
            Object get(Node node) {
                return node.getNextSqnForRecord();
            }

            @Override
            public TypeSerializer<?> getSerializer(TypeSerializer<RowData> serializer) {
                return NULLABLE_LONG_SERIALIZER;
            }
        },
        TIMESTAMP {
            @Override
            Object get(Node node) {
                return node.getTimestamp();
            }

            @Override
            public TypeSerializer<?> getSerializer(TypeSerializer<RowData> serializer) {
                return LONG_SERIALIZER;
            }
        };

        private static TypeSerializer<?>[] getFieldSerializers(TypeSerializer<RowData> serializer) {
            List<TypeSerializer<?>> result = new ArrayList<>();
            for (NodeField field : values()) {
                result.add(field.getSerializer(serializer));
            }
            return result.toArray(new TypeSerializer[0]);
        }

        public abstract TypeSerializer<?> getSerializer(TypeSerializer<RowData> serializer);

        abstract Object get(Node node);

        <T> T get(Object... values) {
            //noinspection unchecked
            return (T) values[ordinal()];
        }

        public static Object get(Node node, int field) {
            return values()[field].get(node);
        }
    }

    /** {@link TypeSerializerSnapshot} of {@link NodeSerializerSnapshot}. */
    public static class NodeSerializerSnapshot
            extends CompositeTypeSerializerSnapshot<Node, NodeSerializer> {
        @SuppressWarnings("unused")
        public NodeSerializerSnapshot() {}

        NodeSerializerSnapshot(NodeSerializer nodeSerializer) {
            super(nodeSerializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return 0;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(NodeSerializer outerSerializer) {
            return outerSerializer.fieldSerializers;
        }

        @Override
        protected NodeSerializer createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            return new NodeSerializer(nestedSerializers);
        }
    }
}

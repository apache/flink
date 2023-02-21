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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.util.function.SerializableSupplier;

import java.util.Objects;

/**
 * The type information for the {@link CommittableMessage} to make it consumable from a {@link
 * org.apache.flink.streaming.api.datastream.DataStream}.
 */
@Experimental
public class CommittableMessageTypeInfo<CommT> extends TypeInformation<CommittableMessage<CommT>> {

    private final SerializableSupplier<SimpleVersionedSerializer<CommT>>
            committableSerializerFactory;

    private CommittableMessageTypeInfo(
            SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializerFactory) {
        this.committableSerializerFactory = committableSerializerFactory;
    }

    /**
     * Returns the type information based on the serializer for a {@link CommittableMessage}.
     *
     * @param committableSerializerFactory factory to create the serializer for a {@link
     *     CommittableMessage}
     * @param <CommT> type of the committable
     * @return
     */
    public static <CommT> TypeInformation<CommittableMessage<CommT>> of(
            SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializerFactory) {
        return new CommittableMessageTypeInfo<>(committableSerializerFactory);
    }

    /**
     * Returns the type information for a {@link CommittableMessage} with no committable.
     *
     * @return {@link TypeInformation} with {@link CommittableMessage}
     */
    public static TypeInformation<CommittableMessage<Void>> noOutput() {
        return new CommittableMessageTypeInfo<>(NoOutputSerializer::new);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<CommittableMessage<CommT>> getTypeClass() {
        return (Class) CommittableMessage.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<CommittableMessage<CommT>> createSerializer(ExecutionConfig config) {
        // no copy, so that data from writer is directly going into committer while chaining
        return new SimpleVersionedSerializerTypeSerializerProxy<CommittableMessage<CommT>>(
                () -> new CommittableMessageSerializer<>(committableSerializerFactory.get())) {
            @Override
            public CommittableMessage<CommT> copy(CommittableMessage<CommT> from) {
                return from;
            }

            @Override
            public CommittableMessage<CommT> copy(
                    CommittableMessage<CommT> from, CommittableMessage<CommT> reuse) {
                return from;
            }
        };
    }

    @Override
    public String toString() {
        return "CommittableMessageTypeInfo{"
                + "serializer="
                + committableSerializerFactory.get()
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !canEqual(o)) {
            return false;
        }
        CommittableMessageTypeInfo<?> that = (CommittableMessageTypeInfo<?>) o;
        return Objects.equals(
                committableSerializerFactory.get().getClass(),
                that.committableSerializerFactory.get().getClass());
    }

    @Override
    public int hashCode() {
        return Objects.hash(committableSerializerFactory.get().getClass());
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof CommittableMessageTypeInfo;
    }

    private static class NoOutputSerializer implements SimpleVersionedSerializer<Void> {

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Void obj) {
            throw new IllegalStateException("Should not serialize anything");
        }

        @Override
        public Void deserialize(int version, byte[] serialized) {
            throw new IllegalStateException("Should not deserialize anything");
        }
    }
}

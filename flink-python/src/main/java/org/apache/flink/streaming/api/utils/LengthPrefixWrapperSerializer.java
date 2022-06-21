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

package org.apache.flink.streaming.api.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;

/**
 * The {@link TypeSerializer} to serialize data with type {@link T} into length-prefix byte[] that
 * allows Python side to determine how long the record is. This wraps a {@link TypeSerializer} for
 * byte[] and a {@link TypeSerializer} for {@link T}.
 */
public class LengthPrefixWrapperSerializer<T> extends TypeSerializer<T> {

    private final TypeSerializer<byte[]> bytesSerializer;

    private final TypeSerializer<T> dataSerializer;

    private final ByteArrayInputStreamWithPos bais;

    private final DataInputViewStreamWrapper baisWrapper;

    private final ByteArrayOutputStreamWithPos baos;

    private final DataOutputViewStreamWrapper baosWrapper;

    public LengthPrefixWrapperSerializer(
            TypeSerializer<byte[]> bytesSerializer, TypeSerializer<T> dataSerializer) {
        this.bytesSerializer = bytesSerializer;
        this.dataSerializer = dataSerializer;
        this.bais = new ByteArrayInputStreamWithPos();
        this.baisWrapper = new DataInputViewStreamWrapper(this.bais);
        this.baos = new ByteArrayOutputStreamWithPos();
        this.baosWrapper = new DataOutputViewStreamWrapper(this.baos);
    }

    @Override
    public boolean isImmutableType() {
        return dataSerializer.isImmutableType() && bytesSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new LengthPrefixWrapperSerializer<>(
                bytesSerializer.duplicate(), dataSerializer.duplicate());
    }

    @Override
    public T createInstance() {
        return dataSerializer.createInstance();
    }

    @Override
    public T copy(T from) {
        return dataSerializer.copy(from);
    }

    @Override
    public T copy(T from, T reuse) {
        return dataSerializer.copy(from, reuse);
    }

    @Override
    public int getLength() {
        return bytesSerializer.getLength();
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        dataSerializer.serialize(record, baosWrapper);
        bytesSerializer.serialize(baos.toByteArray(), target);
        baos.reset();
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        byte[] bytes = bytesSerializer.deserialize(source);
        bais.setBuffer(bytes, 0, bytes.length);
        return dataSerializer.deserialize(baisWrapper);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        byte[] bytes = bytesSerializer.deserialize(source);
        bais.setBuffer(bytes, 0, bytes.length);
        return dataSerializer.deserialize(reuse, baisWrapper);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        bytesSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj.getClass().equals(this.getClass()) && bytesSerializer.equals(obj);
    }

    @Override
    public int hashCode() {
        return bytesSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new LengthPrefixWrapperSerializerSnapshot<>(
                bytesSerializer.snapshotConfiguration(), dataSerializer.snapshotConfiguration());
    }

    /**
     * {@link LengthPrefixWrapperSerializerSnapshot} wraps a serializer snapshot for byte[] and
     * another for {@link T}.
     */
    public static class LengthPrefixWrapperSerializerSnapshot<T>
            implements TypeSerializerSnapshot<T> {

        private final TypeSerializerSnapshot<byte[]> bytesSerializerSnapshot;

        private final TypeSerializerSnapshot<T> dataSerializerSnapshot;

        public LengthPrefixWrapperSerializerSnapshot(
                TypeSerializerSnapshot<byte[]> bytesSerializerSnapshot,
                TypeSerializerSnapshot<T> dataSerializerSnapshot) {
            this.bytesSerializerSnapshot = bytesSerializerSnapshot;
            this.dataSerializerSnapshot = dataSerializerSnapshot;
        }

        @Override
        public int getCurrentVersion() {
            return dataSerializerSnapshot.getCurrentVersion();
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            bytesSerializerSnapshot.writeSnapshot(out);
            dataSerializerSnapshot.writeSnapshot(out);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            bytesSerializerSnapshot.readSnapshot(readVersion, in, userCodeClassLoader);
            dataSerializerSnapshot.readSnapshot(readVersion, in, userCodeClassLoader);
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            return new LengthPrefixWrapperSerializer<>(
                    bytesSerializerSnapshot.restoreSerializer(),
                    dataSerializerSnapshot.restoreSerializer());
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializer<T> newSerializer) {
            if (!(newSerializer instanceof LengthPrefixWrapperSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            // bytes serializer is considered always compatible
            TypeSerializerSchemaCompatibility<T> dataSerializerCompatibility =
                    dataSerializerSnapshot.resolveSchemaCompatibility(newSerializer);
            if (dataSerializerCompatibility.isCompatibleWithReconfiguredSerializer()) {
                return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                        new LengthPrefixWrapperSerializer<>(
                                ((LengthPrefixWrapperSerializer<T>) newSerializer).bytesSerializer,
                                dataSerializerCompatibility.getReconfiguredSerializer()));
            } else {
                return dataSerializerCompatibility;
            }
        }
    }
}

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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
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
@Internal
public class LengthPrefixWrapperSerializer<T> extends TypeSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final TypeSerializer<byte[]> bytesSerializer;

    private final TypeSerializer<T> dataSerializer;

    private transient ByteArrayInputStreamWithPos bais;

    private transient DataInputViewStreamWrapper baisWrapper;

    private transient ByteArrayOutputStreamWithPos baos;

    private transient DataOutputViewStreamWrapper baosWrapper;

    public LengthPrefixWrapperSerializer(TypeSerializer<T> dataSerializer) {
        this(BytePrimitiveArraySerializer.INSTANCE, dataSerializer);
    }

    private LengthPrefixWrapperSerializer(
            TypeSerializer<byte[]> bytesSerializer, TypeSerializer<T> dataSerializer) {
        this.bytesSerializer = bytesSerializer;
        this.dataSerializer = dataSerializer;
        initializeBuffer();
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
        if (dataSerializer.getLength() == -1) {
            return -1;
        } else {
            return dataSerializer.getLength() + Integer.BYTES;
        }
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
        if (obj == this) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        final LengthPrefixWrapperSerializer objSerializer = (LengthPrefixWrapperSerializer) obj;
        return bytesSerializer.equals(objSerializer.getBytesSerializer())
                && dataSerializer.equals(objSerializer.getDataSerializer());
    }

    @Override
    public int hashCode() {
        return bytesSerializer.hashCode() + dataSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new LengthPrefixWrapperSerializerSnapshot<>(
                bytesSerializer.snapshotConfiguration(), dataSerializer.snapshotConfiguration());
    }

    public TypeSerializer<byte[]> getBytesSerializer() {
        return bytesSerializer;
    }

    public TypeSerializer<T> getDataSerializer() {
        return dataSerializer;
    }

    private void initializeBuffer() {
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(this.bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(this.baos);
    }

    /**
     * {@link LengthPrefixWrapperSerializerSnapshot} wraps a serializer snapshot for byte[] and
     * another for {@link T}.
     */
    public static class LengthPrefixWrapperSerializerSnapshot<T>
            implements TypeSerializerSnapshot<T> {

        private static final int VERSION = 1;

        private TypeSerializerSnapshot<byte[]> bytesSerializerSnapshot;

        private TypeSerializerSnapshot<T> dataSerializerSnapshot;

        public LengthPrefixWrapperSerializerSnapshot() {}

        public LengthPrefixWrapperSerializerSnapshot(
                TypeSerializerSnapshot<byte[]> bytesSerializerSnapshot,
                TypeSerializerSnapshot<T> dataSerializerSnapshot) {
            this.bytesSerializerSnapshot = bytesSerializerSnapshot;
            this.dataSerializerSnapshot = dataSerializerSnapshot;
        }

        @Override
        public int getCurrentVersion() {
            return VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            TypeSerializerSnapshot.writeVersionedSnapshot(out, bytesSerializerSnapshot);
            TypeSerializerSnapshot.writeVersionedSnapshot(out, dataSerializerSnapshot);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readVersion != VERSION) {
                throw new IllegalArgumentException(
                        "unknown snapshot version for AvroSerializerSnapshot " + readVersion);
            }
            bytesSerializerSnapshot =
                    TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader);
            dataSerializerSnapshot =
                    TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader);
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            LengthPrefixWrapperSerializer<T> serializer =
                    new LengthPrefixWrapperSerializer<>(
                            bytesSerializerSnapshot.restoreSerializer(),
                            dataSerializerSnapshot.restoreSerializer());
            serializer.initializeBuffer();
            return serializer;
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializer<T> newSerializer) {
            if (!(newSerializer instanceof LengthPrefixWrapperSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            final LengthPrefixWrapperSerializer<T> lengthPrefixWrapperSerializer =
                    (LengthPrefixWrapperSerializer<T>) newSerializer;
            // bytes serializer is considered always compatible
            TypeSerializerSchemaCompatibility<T> dataSerializerCompatibility =
                    dataSerializerSnapshot.resolveSchemaCompatibility(
                            lengthPrefixWrapperSerializer.getDataSerializer());
            if (dataSerializerCompatibility.isCompatibleWithReconfiguredSerializer()) {
                return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                        new LengthPrefixWrapperSerializer<>(
                                lengthPrefixWrapperSerializer.getBytesSerializer(),
                                dataSerializerCompatibility.getReconfiguredSerializer()));
            } else {
                return dataSerializerCompatibility;
            }
        }
    }
}

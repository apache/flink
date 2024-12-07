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

package org.apache.flink.streaming.api.operators.sortpartition;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * {@link KeyAndValueSerializer} is used in {@link KeyedSortPartitionOperator} for serializing
 * elements including key and record. It serializes the record in a format known by the {@link
 * FixedLengthByteKeyAndValueComparator} and {@link VariableLengthByteKeyAndValueComparator}.
 *
 * <p>If the key's length is fixed and known, the format of each serialized element is as follows:
 *
 * <pre>
 *      [key] | [record]
 * </pre>
 *
 * <p>If the key's length is variable, the format of each serialized element is as follows:
 *
 * <pre>
 *      [key's length] | [key] | [record]
 * </pre>
 */
final class KeyAndValueSerializer<INPUT> extends TypeSerializer<Tuple2<byte[], INPUT>> {
    private final TypeSerializer<INPUT> valueSerializer;

    /**
     * The value of serializedKeyLength will be positive if the key's length is fixed otherwise -1.
     */
    private final int serializedKeyLength;

    KeyAndValueSerializer(TypeSerializer<INPUT> valueSerializer, int serializedKeyLength) {
        this.valueSerializer = valueSerializer;
        this.serializedKeyLength = serializedKeyLength;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Tuple2<byte[], INPUT>> duplicate() {
        return new KeyAndValueSerializer<>(valueSerializer.duplicate(), this.serializedKeyLength);
    }

    @Override
    public Tuple2<byte[], INPUT> copy(Tuple2<byte[], INPUT> from) {
        INPUT record = from.f1;
        return Tuple2.of(Arrays.copyOf(from.f0, from.f0.length), valueSerializer.copy(record));
    }

    @Override
    public Tuple2<byte[], INPUT> createInstance() {
        return Tuple2.of(new byte[0], valueSerializer.createInstance());
    }

    @Override
    public Tuple2<byte[], INPUT> copy(Tuple2<byte[], INPUT> from, Tuple2<byte[], INPUT> reuse) {
        reuse.f0 = Arrays.copyOf(from.f0, from.f0.length);
        INPUT fromRecord = from.f1;
        reuse.f1 = valueSerializer.copy(fromRecord, reuse.f1);
        return reuse;
    }

    @Override
    public int getLength() {
        if (valueSerializer.getLength() < 0 || serializedKeyLength < 0) {
            return -1;
        }
        return valueSerializer.getLength() + serializedKeyLength;
    }

    @Override
    public void serialize(Tuple2<byte[], INPUT> record, DataOutputView target) throws IOException {
        if (serializedKeyLength < 0) {
            target.writeInt(record.f0.length);
        }
        target.write(record.f0);
        INPUT toSerialize = record.f1;
        valueSerializer.serialize(toSerialize, target);
    }

    @Override
    public Tuple2<byte[], INPUT> deserialize(DataInputView source) throws IOException {
        final int length = getKeyLength(source);
        byte[] bytes = new byte[length];
        source.read(bytes);
        INPUT value = valueSerializer.deserialize(source);
        return Tuple2.of(bytes, value);
    }

    @Override
    public Tuple2<byte[], INPUT> deserialize(Tuple2<byte[], INPUT> reuse, DataInputView source)
            throws IOException {
        final int length = getKeyLength(source);
        byte[] bytes = new byte[length];
        source.read(bytes);
        INPUT value = valueSerializer.deserialize(source);
        reuse.f0 = bytes;
        reuse.f1 = value;
        return reuse;
    }

    private int getKeyLength(DataInputView source) throws IOException {
        final int length;
        if (serializedKeyLength < 0) {
            length = source.readInt();
        } else {
            length = serializedKeyLength;
        }
        return length;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int length;
        if (serializedKeyLength < 0) {
            length = source.readInt();
            target.writeInt(length);
        } else {
            length = serializedKeyLength;
        }
        for (int i = 0; i < length; i++) {
            target.writeByte(source.readByte());
        }
        valueSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyAndValueSerializer<?> that = (KeyAndValueSerializer<?>) o;
        return Objects.equals(valueSerializer, that.valueSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueSerializer);
    }

    @Override
    public TypeSerializerSnapshot<Tuple2<byte[], INPUT>> snapshotConfiguration() {
        throw new UnsupportedOperationException();
    }
}

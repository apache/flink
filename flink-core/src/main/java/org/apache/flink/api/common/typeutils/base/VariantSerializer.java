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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.Variant;

import java.io.IOException;
import java.util.Arrays;

/** Serializer for {@link Variant}. */
@Internal
public class VariantSerializer extends TypeSerializerSingleton<Variant> {

    public static final VariantSerializer INSTANCE = new VariantSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public Variant createInstance() {
        return Variant.newBuilder().ofNull();
    }

    @Override
    public Variant copy(Variant from) {
        BinaryVariant binaryVariant = toBinaryVariant(from);
        return new BinaryVariant(
                Arrays.copyOf(binaryVariant.getValue(), binaryVariant.getValue().length),
                Arrays.copyOf(binaryVariant.getMetadata(), binaryVariant.getMetadata().length));
    }

    @Override
    public Variant copy(Variant from, Variant reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Variant record, DataOutputView target) throws IOException {

        BinaryVariant binaryVariant = toBinaryVariant(record);

        target.writeInt(binaryVariant.getValue().length);
        target.writeInt(binaryVariant.getMetadata().length);
        target.write(binaryVariant.getValue());
        target.write(binaryVariant.getMetadata());
    }

    @Override
    public Variant deserialize(DataInputView source) throws IOException {
        int valueLength = source.readInt();
        int metadataLength = source.readInt();
        byte[] value = new byte[valueLength];
        byte[] metaData = new byte[metadataLength];

        source.read(value);
        source.read(metaData);
        return new BinaryVariant(value, metaData);
    }

    @Override
    public Variant deserialize(Variant reuse, DataInputView source) throws IOException {
        return this.deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int valueLength = source.readInt();
        int metadataLength = source.readInt();
        target.writeInt(valueLength);
        target.writeInt(metadataLength);
        target.write(source, valueLength + metadataLength);
    }

    @Override
    public TypeSerializerSnapshot<Variant> snapshotConfiguration() {
        return new VariantSerializerSnapshot();
    }

    private BinaryVariant toBinaryVariant(Variant variant) {
        if (variant instanceof BinaryVariant) {
            return (BinaryVariant) variant;
        }

        throw new UnsupportedOperationException("Unsupported variant type: " + variant.getClass());
    }

    @Internal
    public static final class VariantSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Variant> {
        /** Constructor to create snapshot from serializer (writing the snapshot). */
        public VariantSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}

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

import java.io.IOException;
import java.util.UUID;

/**
 * Serializer for {@link UUID}.
 *
 * <p>A UUID is written as its canonical 16-byte big-endian form: the most significant 64 bits
 * followed by the least significant 64 bits. All 128 bits are valid, so there is no reserved value
 * for {@code null}; nullability is handled by wrapping serializers (e.g. {@code NullableSerializer}
 * or the row/POJO serializers) as for the other fixed-width base serializers.
 */
@Internal
public final class UuidSerializer extends TypeSerializerSingleton<UUID> {

    private static final long serialVersionUID = 1L;

    /** Length of a serialized UUID: two 64-bit longs. */
    static final int UUID_BYTES = 2 * Long.BYTES;

    public static final UuidSerializer INSTANCE = new UuidSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public UUID createInstance() {
        return new UUID(0L, 0L);
    }

    @Override
    public UUID copy(UUID from) {
        return from;
    }

    @Override
    public UUID copy(UUID from, UUID reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return UUID_BYTES;
    }

    @Override
    public void serialize(UUID record, DataOutputView target) throws IOException {
        target.writeLong(record.getMostSignificantBits());
        target.writeLong(record.getLeastSignificantBits());
    }

    @Override
    public UUID deserialize(DataInputView source) throws IOException {
        final long mostSignificantBits = source.readLong();
        final long leastSignificantBits = source.readLong();
        return new UUID(mostSignificantBits, leastSignificantBits);
    }

    @Override
    public UUID deserialize(UUID reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
        target.writeLong(source.readLong());
    }

    @Override
    public TypeSerializerSnapshot<UUID> snapshotConfiguration() {
        return new UuidSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @Internal
    public static final class UuidSerializerSnapshot extends SimpleTypeSerializerSnapshot<UUID> {

        public UuidSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}

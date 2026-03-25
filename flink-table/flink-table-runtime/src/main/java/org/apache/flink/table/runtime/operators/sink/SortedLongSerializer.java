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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.MathUtils;

import java.io.IOException;

/**
 * A serializer for {@code Long} that produces a lexicographically sortable byte representation.
 *
 * <p>Standard big-endian long serialization does not maintain numeric ordering for negative values
 * in lexicographic byte comparison because negative numbers have their sign bit set (1), which
 * makes them appear greater than positive numbers (sign bit 0) in unsigned byte comparison.
 *
 * <p>This serializer flips the sign bit during serialization, converting from signed to unsigned
 * ordering. This ensures that when iterating over keys in a sorted state backend (like RocksDB),
 * the entries are returned in numeric order.
 *
 * @see org.apache.flink.streaming.api.operators.TimerSerializer
 */
@Internal
public final class SortedLongSerializer extends TypeSerializerSingleton<Long> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the SortedLongSerializer. */
    public static final SortedLongSerializer INSTANCE = new SortedLongSerializer();

    private static final Long ZERO = 0L;

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public Long createInstance() {
        return ZERO;
    }

    @Override
    public Long copy(Long from) {
        return from;
    }

    @Override
    public Long copy(Long from, Long reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return Long.BYTES;
    }

    @Override
    public void serialize(Long record, DataOutputView target) throws IOException {
        target.writeLong(MathUtils.flipSignBit(record));
    }

    @Override
    public Long deserialize(DataInputView source) throws IOException {
        return MathUtils.flipSignBit(source.readLong());
    }

    @Override
    public Long deserialize(Long reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
    }

    @Override
    public TypeSerializerSnapshot<Long> snapshotConfiguration() {
        return new SortedLongSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class SortedLongSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Long> {

        public SortedLongSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}

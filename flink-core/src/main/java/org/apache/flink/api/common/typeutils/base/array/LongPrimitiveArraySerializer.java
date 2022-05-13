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

package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A serializer for long arrays. */
@Internal
public final class LongPrimitiveArraySerializer extends TypeSerializerSingleton<long[]> {

    private static final long serialVersionUID = 1L;

    private static final long[] EMPTY = new long[0];

    public static final LongPrimitiveArraySerializer INSTANCE = new LongPrimitiveArraySerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public long[] createInstance() {
        return EMPTY;
    }

    @Override
    public long[] copy(long[] from) {
        long[] result = new long[from.length];
        System.arraycopy(from, 0, result, 0, from.length);
        return result;
    }

    @Override
    public long[] copy(long[] from, long[] reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(long[] record, DataOutputView target) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("The record must not be null.");
        }

        final int len = record.length;
        target.writeInt(len);
        for (int i = 0; i < len; i++) {
            target.writeLong(record[i]);
        }
    }

    @Override
    public long[] deserialize(DataInputView source) throws IOException {
        final int len = source.readInt();
        long[] array = new long[len];

        for (int i = 0; i < len; i++) {
            array[i] = source.readLong();
        }

        return array;
    }

    @Override
    public long[] deserialize(long[] reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int len = source.readInt();
        target.writeInt(len);
        target.write(source, len * 8);
    }

    @Override
    public TypeSerializerSnapshot<long[]> snapshotConfiguration() {
        return new LongPrimitiveArraySerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class LongPrimitiveArraySerializerSnapshot
            extends SimpleTypeSerializerSnapshot<long[]> {

        public LongPrimitiveArraySerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}

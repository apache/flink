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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.runtime.operators.sink.SortedLongSerializer;
import org.apache.flink.util.MathUtils;

import java.io.IOException;

/**
 * Serializer for {@link LeftTimeIndexKey} that produces a lexicographically sortable byte
 * representation.
 *
 * @see SortedLongSerializer
 */
@Internal
public final class LeftTimeIndexKeySerializer extends TypeSerializerSingleton<LeftTimeIndexKey> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the LeftTimeIndexKeySerializer. */
    public static final LeftTimeIndexKeySerializer INSTANCE = new LeftTimeIndexKeySerializer();

    private static final LeftTimeIndexKey ZERO = new LeftTimeIndexKey(0L, 0L);

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public LeftTimeIndexKey createInstance() {
        return ZERO;
    }

    @Override
    public LeftTimeIndexKey copy(LeftTimeIndexKey from) {
        return from;
    }

    @Override
    public LeftTimeIndexKey copy(LeftTimeIndexKey from, LeftTimeIndexKey reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 2 * Long.BYTES;
    }

    @Override
    public void serialize(LeftTimeIndexKey record, DataOutputView target) throws IOException {
        target.writeLong(MathUtils.flipSignBit(record.getTimestamp()));
        target.writeLong(MathUtils.flipSignBit(record.getIndex()));
    }

    @Override
    public LeftTimeIndexKey deserialize(DataInputView source) throws IOException {
        long timestamp = MathUtils.flipSignBit(source.readLong());
        long index = MathUtils.flipSignBit(source.readLong());
        return new LeftTimeIndexKey(timestamp, index);
    }

    @Override
    public LeftTimeIndexKey deserialize(LeftTimeIndexKey reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
        target.writeLong(source.readLong());
    }

    @Override
    public TypeSerializerSnapshot<LeftTimeIndexKey> snapshotConfiguration() {
        return new LeftTimeIndexKeySerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    public static final class LeftTimeIndexKeySerializerSnapshot
            extends SimpleTypeSerializerSnapshot<LeftTimeIndexKey> {

        public LeftTimeIndexKeySerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}

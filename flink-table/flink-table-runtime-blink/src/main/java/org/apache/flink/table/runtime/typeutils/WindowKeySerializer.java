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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.util.WindowKey;

import java.io.IOException;

/**
 * Serializer for {@link WindowKey} which is composite with a {@link BinaryRowData} and a long
 * value.
 */
public class WindowKeySerializer extends PagedTypeSerializer<WindowKey> {
    private static final long serialVersionUID = 1L;
    public static final int WINDOW_IN_BYTES = 8;

    private final BinaryRowDataSerializer keySerializer;

    public WindowKeySerializer(int numKeyFields) {
        this(new BinaryRowDataSerializer(numKeyFields));
    }

    public WindowKeySerializer(BinaryRowDataSerializer keySerializer) {
        this.keySerializer = keySerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<WindowKey> duplicate() {
        return new WindowKeySerializer(keySerializer.getArity());
    }

    @Override
    public WindowKey createInstance() {
        return new WindowKey(Long.MIN_VALUE, keySerializer.createInstance());
    }

    @Override
    public WindowKey copy(WindowKey from) {
        return new WindowKey(from.getWindow(), keySerializer.copy(from.getKey()));
    }

    @Override
    public WindowKey copy(WindowKey from, WindowKey reuse) {
        long window = from.getWindow();
        BinaryRowData key = keySerializer.copy(from.getKey(), reuse.getKey());
        return reuse.replace(window, key);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(WindowKey record, DataOutputView target) throws IOException {
        target.writeLong(record.getWindow());
        keySerializer.serialize(record.getKey(), target);
    }

    @Override
    public WindowKey deserialize(DataInputView source) throws IOException {
        long window = source.readLong();
        BinaryRowData key = keySerializer.deserialize(source);
        return new WindowKey(window, key);
    }

    @Override
    public WindowKey deserialize(WindowKey reuse, DataInputView source) throws IOException {
        long window = source.readLong();
        BinaryRowData key = keySerializer.deserialize(reuse.getKey(), source);
        return reuse.replace(window, key);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
        keySerializer.copy(source, target);
    }

    @Override
    public int serializeToPages(WindowKey record, AbstractPagedOutputView target)
            throws IOException {
        int windowSkip = checkSkipWriteForWindowPart(target);
        target.writeLong(record.getWindow());
        int keySkip = keySerializer.serializeToPages(record.getKey(), target);
        return windowSkip + keySkip;
    }

    @Override
    public WindowKey deserializeFromPages(AbstractPagedInputView source) throws IOException {
        return deserializeFromPages(createInstance(), source);
    }

    @Override
    public WindowKey deserializeFromPages(WindowKey reuse, AbstractPagedInputView source)
            throws IOException {
        checkSkipReadForFixLengthPart(source);
        long window = source.readLong();
        BinaryRowData key = keySerializer.deserializeFromPages(reuse.getKey(), source);
        return reuse.replace(window, key);
    }

    @Override
    public WindowKey mapFromPages(WindowKey reuse, AbstractPagedInputView source)
            throws IOException {
        checkSkipReadForFixLengthPart(source);
        long window = source.readLong();
        BinaryRowData key = keySerializer.mapFromPages(reuse.getKey(), source);
        return reuse.replace(window, key);
    }

    @Override
    public void skipRecordFromPages(AbstractPagedInputView source) throws IOException {
        checkSkipReadForFixLengthPart(source);
        source.skipBytes(WINDOW_IN_BYTES);
        keySerializer.skipRecordFromPages(source);
    }

    /**
     * We need skip bytes to write when the remain bytes of current segment is not enough to write
     * window part.
     */
    private int checkSkipWriteForWindowPart(AbstractPagedOutputView out) throws IOException {
        // skip if there is no enough size.
        int available = out.getSegmentSize() - out.getCurrentPositionInSegment();
        if (available < getSerializedFixedPartLength()) {
            out.advance();
            return available;
        }
        return 0;
    }

    /**
     * We need skip bytes to read when the remain bytes of current segment is not enough to read
     * window part.
     */
    private void checkSkipReadForFixLengthPart(AbstractPagedInputView source) throws IOException {
        // skip if there is no enough size.
        // Note: Use currentSegmentLimit instead of segmentSize.
        int available = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
        if (available < getSerializedFixedPartLength()) {
            source.advance();
        }
    }

    private int getSerializedFixedPartLength() {
        return WINDOW_IN_BYTES + keySerializer.getSerializedRowFixedPartLength();
    }

    // ------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        return obj instanceof WindowKeySerializer
                && keySerializer.equals(((WindowKeySerializer) obj).keySerializer);
    }

    @Override
    public int hashCode() {
        return keySerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<WindowKey> snapshotConfiguration() {
        return new WindowKeySerializerSnapshot(this);
    }

    /** A {@link TypeSerializerSnapshot} for {@link WindowKeySerializer}. */
    public static final class WindowKeySerializerSnapshot
            extends CompositeTypeSerializerSnapshot<WindowKey, WindowKeySerializer> {

        private static final int CURRENT_VERSION = 1;

        /** This empty nullary constructor is required for deserializing the configuration. */
        public WindowKeySerializerSnapshot() {
            super(WindowKeySerializer.class);
        }

        public WindowKeySerializerSnapshot(WindowKeySerializer serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return CURRENT_VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(WindowKeySerializer outerSerializer) {
            return new TypeSerializer[] {outerSerializer.keySerializer};
        }

        @Override
        protected WindowKeySerializer createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            return new WindowKeySerializer((BinaryRowDataSerializer) nestedSerializers[0]);
        }
    }
}

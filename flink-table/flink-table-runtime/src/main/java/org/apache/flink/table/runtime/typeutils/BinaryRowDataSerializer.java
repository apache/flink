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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentWritable;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Serializer for {@link BinaryRowData}. */
@Internal
public class BinaryRowDataSerializer extends AbstractRowDataSerializer<BinaryRowData> {

    private static final long serialVersionUID = 1L;
    public static final int LENGTH_SIZE_IN_BYTES = 4;

    private final int numFields;
    private final int fixedLengthPartSize;

    public BinaryRowDataSerializer(int numFields) {
        this.numFields = numFields;
        this.fixedLengthPartSize = BinaryRowData.calculateFixPartSizeInBytes(numFields);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<BinaryRowData> duplicate() {
        return new BinaryRowDataSerializer(numFields);
    }

    @Override
    public BinaryRowData createInstance() {
        return new BinaryRowData(numFields);
    }

    @Override
    public BinaryRowData copy(BinaryRowData from) {
        return copy(from, new BinaryRowData(numFields));
    }

    @Override
    public BinaryRowData copy(BinaryRowData from, BinaryRowData reuse) {
        return from.copy(reuse);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(BinaryRowData record, DataOutputView target) throws IOException {
        target.writeInt(record.getSizeInBytes());
        if (target instanceof MemorySegmentWritable) {
            serializeWithoutLength(record, (MemorySegmentWritable) target);
        } else {
            BinarySegmentUtils.copyToView(
                    record.getSegments(), record.getOffset(), record.getSizeInBytes(), target);
        }
    }

    @Override
    public BinaryRowData deserialize(DataInputView source) throws IOException {
        BinaryRowData row = new BinaryRowData(numFields);
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        row.pointTo(MemorySegmentFactory.wrap(bytes), 0, length);
        return row;
    }

    @Override
    public BinaryRowData deserialize(BinaryRowData reuse, DataInputView source) throws IOException {
        MemorySegment[] segments = reuse.getSegments();
        checkArgument(
                segments == null || (segments.length == 1 && reuse.getOffset() == 0),
                "Reuse BinaryRowData should have no segments or only one segment and offset start at 0.");

        int length = source.readInt();
        if (segments == null || segments[0].size() < length) {
            segments = new MemorySegment[] {MemorySegmentFactory.wrap(new byte[length])};
        }
        source.readFully(segments[0].getArray(), 0, length);
        reuse.pointTo(segments, 0, length);
        return reuse;
    }

    @Override
    public int getArity() {
        return numFields;
    }

    @Override
    public BinaryRowData toBinaryRow(BinaryRowData rowData) throws IOException {
        return rowData;
    }

    // ============================ Page related operations ===================================

    @Override
    public int serializeToPages(BinaryRowData record, AbstractPagedOutputView headerLessView)
            throws IOException {
        checkArgument(headerLessView.getHeaderLength() == 0);
        int skip = checkSkipWriteForFixLengthPart(headerLessView);
        headerLessView.writeInt(record.getSizeInBytes());
        serializeWithoutLength(record, headerLessView);
        return skip;
    }

    private static void serializeWithoutLength(BinaryRowData record, MemorySegmentWritable writable)
            throws IOException {
        if (record.getSegments().length == 1) {
            writable.write(record.getSegments()[0], record.getOffset(), record.getSizeInBytes());
        } else {
            serializeWithoutLengthSlow(record, writable);
        }
    }

    public static void serializeWithoutLengthSlow(BinaryRowData record, MemorySegmentWritable out)
            throws IOException {
        int remainSize = record.getSizeInBytes();
        int posInSegOfRecord = record.getOffset();
        int segmentSize = record.getSegments()[0].size();
        for (MemorySegment segOfRecord : record.getSegments()) {
            int nWrite = Math.min(segmentSize - posInSegOfRecord, remainSize);
            assert nWrite > 0;
            out.write(segOfRecord, posInSegOfRecord, nWrite);

            // next new segment.
            posInSegOfRecord = 0;
            remainSize -= nWrite;
            if (remainSize == 0) {
                break;
            }
        }
        checkArgument(remainSize == 0);
    }

    @Override
    public BinaryRowData deserializeFromPages(AbstractPagedInputView headerLessView)
            throws IOException {
        return deserializeFromPages(createInstance(), headerLessView);
    }

    @Override
    public BinaryRowData deserializeFromPages(
            BinaryRowData reuse, AbstractPagedInputView headerLessView) throws IOException {
        checkArgument(headerLessView.getHeaderLength() == 0);
        checkSkipReadForFixLengthPart(headerLessView);
        return deserialize(reuse, headerLessView);
    }

    @Override
    public BinaryRowData mapFromPages(BinaryRowData reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        checkArgument(headerLessView.getHeaderLength() == 0);
        checkSkipReadForFixLengthPart(headerLessView);
        pointTo(headerLessView.readInt(), reuse, headerLessView);
        return reuse;
    }

    @Override
    public void skipRecordFromPages(AbstractPagedInputView headerLessView) throws IOException {
        checkArgument(headerLessView.getHeaderLength() == 0);
        checkSkipReadForFixLengthPart(headerLessView);
        headerLessView.skipBytes(headerLessView.readInt());
    }

    /**
     * Copy a binaryRow which stored in paged input view to output view.
     *
     * @param source source paged input view where the binary row stored
     * @param target the target output view.
     */
    public void copyFromPagesToView(AbstractPagedInputView source, DataOutputView target)
            throws IOException {
        checkSkipReadForFixLengthPart(source);
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    /**
     * Point row to memory segments with offset(in the AbstractPagedInputView) and length.
     *
     * @param length row length.
     * @param reuse reuse BinaryRowData object.
     * @param headerLessView source memory segments container.
     */
    public void pointTo(int length, BinaryRowData reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        checkArgument(headerLessView.getHeaderLength() == 0);
        if (length < 0) {
            throw new IOException(
                    String.format(
                            "Read unexpected bytes in source of positionInSegment[%d] and limitInSegment[%d]",
                            headerLessView.getCurrentPositionInSegment(),
                            headerLessView.getCurrentSegmentLimit()));
        }

        int remainInSegment =
                headerLessView.getCurrentSegmentLimit()
                        - headerLessView.getCurrentPositionInSegment();
        MemorySegment currSeg = headerLessView.getCurrentSegment();
        int currPosInSeg = headerLessView.getCurrentPositionInSegment();
        if (remainInSegment >= length) {
            // all in one segment, that's good.
            reuse.pointTo(currSeg, currPosInSeg, length);
            headerLessView.skipBytesToRead(length);
        } else {
            pointToMultiSegments(
                    reuse, headerLessView, length, length - remainInSegment, currSeg, currPosInSeg);
        }
    }

    private void pointToMultiSegments(
            BinaryRowData reuse,
            AbstractPagedInputView source,
            int sizeInBytes,
            int remainLength,
            MemorySegment currSeg,
            int currPosInSeg)
            throws IOException {

        int segmentSize = currSeg.size();
        int div = remainLength / segmentSize;
        int remainder = remainLength - segmentSize * div; // equal to p % q
        int varSegSize = remainder == 0 ? div : div + 1;

        MemorySegment[] segments = new MemorySegment[varSegSize + 1];
        segments[0] = currSeg;
        for (int i = 1; i <= varSegSize; i++) {
            source.advance();
            segments[i] = source.getCurrentSegment();
        }

        // The remaining is 0. There is no next Segment at this time. The current Segment is
        // all the data of this row, so we need to skip segmentSize bytes to read. We can't
        // jump directly to the next Segment. Because maybe there are no segment in later.
        int remainLenInLastSeg = remainder == 0 ? segmentSize : remainder;
        source.skipBytesToRead(remainLenInLastSeg);
        reuse.pointTo(segments, currPosInSeg, sizeInBytes);
    }

    /**
     * We need skip bytes to write when the remain bytes of current segment is not enough to write
     * binary row fixed part. See {@link BinaryRowData}.
     */
    private int checkSkipWriteForFixLengthPart(AbstractPagedOutputView out) throws IOException {
        // skip if there is no enough size.
        int available = out.getSegmentSize() - out.getCurrentPositionInSegment();
        if (available < getSerializedRowFixedPartLength()) {
            out.advance();
            return available;
        }
        return 0;
    }

    /**
     * We need skip bytes to read when the remain bytes of current segment is not enough to write
     * binary row fixed part. See {@link BinaryRowData}.
     */
    public void checkSkipReadForFixLengthPart(AbstractPagedInputView source) throws IOException {
        // skip if there is no enough size.
        // Note: Use currentSegmentLimit instead of segmentSize.
        int available = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
        if (available < getSerializedRowFixedPartLength()) {
            source.advance();
        }
    }

    /** Return fixed part length to serialize one row. */
    public int getSerializedRowFixedPartLength() {
        return getFixedLengthPartSize() + LENGTH_SIZE_IN_BYTES;
    }

    public int getFixedLengthPartSize() {
        return fixedLengthPartSize;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BinaryRowDataSerializer
                && numFields == ((BinaryRowDataSerializer) obj).numFields;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(numFields);
    }

    @Override
    public TypeSerializerSnapshot<BinaryRowData> snapshotConfiguration() {
        return new BinaryRowDataSerializerSnapshot(numFields);
    }

    /** {@link TypeSerializerSnapshot} for {@link BinaryRowDataSerializer}. */
    public static final class BinaryRowDataSerializerSnapshot
            implements TypeSerializerSnapshot<BinaryRowData> {
        private static final int CURRENT_VERSION = 3;

        private int previousNumFields;

        @SuppressWarnings("unused")
        public BinaryRowDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        BinaryRowDataSerializerSnapshot(int numFields) {
            this.previousNumFields = numFields;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(previousNumFields);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            this.previousNumFields = in.readInt();
        }

        @Override
        public TypeSerializer<BinaryRowData> restoreSerializer() {
            return new BinaryRowDataSerializer(previousNumFields);
        }

        @Override
        public TypeSerializerSchemaCompatibility<BinaryRowData> resolveSchemaCompatibility(
                TypeSerializer<BinaryRowData> newSerializer) {
            if (!(newSerializer instanceof BinaryRowDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            BinaryRowDataSerializer newBinaryRowSerializer =
                    (BinaryRowDataSerializer) newSerializer;
            if (previousNumFields != newBinaryRowSerializer.numFields) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}

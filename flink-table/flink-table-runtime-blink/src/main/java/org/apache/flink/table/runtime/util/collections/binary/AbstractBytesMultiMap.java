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

package org.apache.flink.table.runtime.util.collections.binary;

import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.util.KeyValueIterator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A binary map in the structure like {@code Map<K, ArrayList<V>>}, where there are multiple values
 * under a single key, and they are all bytes based. The design of the value of the binary map
 * changes. It may works like the <@code LinkedList> or <@code ArrayList>. The purpose of the binary
 * map is that can be used for performing aggregations where the accumulator of aggregations are
 * unfixed-width. The memory is divided into three areas:
 *
 * <p>Bucket area: pointer + hashcode
 *
 * <pre>
 *   |---- 4 Bytes (pointer to key entry) ----|
 *   |----- 4 Bytes (key hashcode) ----------|
 * </pre>
 *
 * <p>Key area: a key entry contains key data and other information.
 *
 * <p>Value area: a value entry the value data and other information.
 */
public abstract class AbstractBytesMultiMap<K> extends BytesMap<K, Iterator<RowData>> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBytesMultiMap.class);

    /** Used to serialize map key into RecordArea's MemorySegments. */
    protected final PagedTypeSerializer<K> keySerializer;

    /** Used to serialize hash map key and value into RecordArea's MemorySegments. */
    protected final BinaryRowDataSerializer valueSerializer;

    /** Pointer to the second value under a key. */
    protected int pointerToSecondValue;

    protected BinaryRowData reusedRecord;

    protected long numKeys = 0;

    public AbstractBytesMultiMap(
            final Object owner,
            MemoryManager memoryManager,
            long memorySize,
            PagedTypeSerializer<K> keySerializer,
            LogicalType[] valueTypes) {
        super(owner, memoryManager, memorySize, keySerializer);
        checkArgument(valueTypes.length > 0);

        this.keySerializer = keySerializer;
        this.valueSerializer = new BinaryRowDataSerializer(valueTypes.length);
        this.reusedRecord = valueSerializer.createInstance();

        checkArgument(
                memorySize > INIT_BUCKET_MEMORY_IN_BYTES, "The minBucketMemorySize is not valid!");
        int initBucketSegmentNum =
                MathUtils.roundDownToPowerOf2((int) (INIT_BUCKET_MEMORY_IN_BYTES / segmentSize));

        // allocate and initialize MemorySegments for bucket area
        initBucketSegments(initBucketSegmentNum);

        LOG.info(
                "BytesMultiMap with initial memory segments {}, {} in bytes, init allocating {} for bucket area.",
                reservedNumBuffers,
                reservedNumBuffers * segmentSize,
                initBucketSegmentNum);
    }

    // ----------------------- Abstract Interface -----------------------

    @Override
    public long getNumKeys() {
        return numKeys;
    }

    // ----------------------- Public Interface -----------------------

    /** Append an value into the hash map's record area. */
    public void append(LookupInfo<K, Iterator<RowData>> lookupInfo, BinaryRowData value)
            throws IOException {
        try {
            if (lookupInfo.found) {
                appendRecordWhenFoundKey(lookupInfo, value);
            } else {
                if (numKeys >= growthThreshold) {
                    growAndRehash();
                    // update info's bucketSegmentIndex and bucketOffset
                    lookup(lookupInfo.key);
                }
                // append key and value if it does not exists
                int pointerToAppended = recordArea.appendRecord(lookupInfo, value);
                bucketSegments
                        .get(lookupInfo.bucketSegmentIndex)
                        .putInt(lookupInfo.bucketOffset, pointerToAppended);
                bucketSegments
                        .get(lookupInfo.bucketSegmentIndex)
                        .putInt(
                                lookupInfo.bucketOffset + ELEMENT_POINT_LENGTH,
                                lookupInfo.keyHashCode);
                numKeys++;
            }
            numElements++;
        } catch (EOFException e) {
            numSpillFiles++;
            spillInBytes += recordArea.getSegmentsSize();
            throw e;
        }
    }

    protected abstract void appendRecordWhenFoundKey(
            LookupInfo<K, Iterator<RowData>> info, BinaryRowData value) throws IOException;

    public abstract KeyValueIterator<K, Iterator<RowData>> getEntryIterator();

    /** release the map's record and bucket area's memory segments. */
    public void free() {
        free(false);
    }

    /** @param reservedFixedMemory reserved fixed memory or not. */
    @Override
    public void free(boolean reservedFixedMemory) {
        recordArea.release();
        numKeys = 0;
        super.free(reservedFixedMemory);
    }

    /** reset the map's record and bucket area's memory segments for reusing. */
    @Override
    public void reset() {
        super.reset();
        // reset the record segments.
        recordArea.reset();
        numKeys = 0;
    }

    protected long writeKey(SimpleCollectingOutputView keyOutView, K key) throws IOException {
        int lastPosition = (int) keyOutView.getCurrentOffset();
        // write key to keyOutView
        int skip = keySerializer.serializeToPages(key, keyOutView);
        return lastPosition + skip;
    }

    /** Write value into the output view, and return offset of the value. */
    protected long writePointer(SimpleCollectingOutputView outputView, int value)
            throws IOException {
        int oldPosition = (int) outputView.getCurrentOffset();
        int skip = checkSkipWriteForPointer(outputView);
        outputView.getCurrentSegment().putInt(outputView.getCurrentPositionInSegment(), value);
        // advance position in segment
        outputView.skipBytesToWrite(ELEMENT_POINT_LENGTH);
        return oldPosition + skip;
    }

    protected int readPointer(AbstractPagedInputView inputView) throws IOException {
        checkSkipReadForPointer(inputView);
        int value = inputView.getCurrentSegment().getInt(inputView.getCurrentPositionInSegment());
        // advance position in segment
        inputView.skipBytesToRead(ELEMENT_POINT_LENGTH);
        return value;
    }

    protected int skipPointer(SimpleCollectingOutputView outputView) throws IOException {
        int oldPosition = (int) outputView.getCurrentOffset();
        // skip 4 bytes for pointer to the tail value
        int skip = checkSkipWriteForPointer(outputView);
        outputView.skipBytesToWrite(ELEMENT_POINT_LENGTH);
        return oldPosition + skip;
    }

    protected void skipPointer(AbstractPagedInputView inputView) throws IOException {
        checkSkipReadForPointer(inputView);
        inputView.skipBytesToRead(ELEMENT_POINT_LENGTH);
    }

    /** Update the content from specific offset. */
    protected void updateValuePointer(RandomAccessInputView view, int newPointer, int ptrOffset) {
        view.setReadPosition(ptrOffset);
        int currPosInSeg = view.getCurrentPositionInSegment();
        view.getCurrentSegment().putInt(currPosInSeg, newPointer);
    }

    /** For pointer needing update, skip unaligned part (4 bytes) for convenient updating. */
    private int checkSkipWriteForPointer(AbstractPagedOutputView outView) throws IOException {
        // skip if there is no enough size.
        int available = outView.getSegmentSize() - outView.getCurrentPositionInSegment();
        if (available < ELEMENT_POINT_LENGTH) {
            outView.advance();
            return available;
        }
        return 0;
    }

    /** For pointer needing update, skip unaligned part (4 bytes) for convenient updating. */
    private void checkSkipReadForPointer(AbstractPagedInputView source) throws IOException {
        // skip if there is no enough size.
        // Note: Use currentSegmentLimit instead of segmentSize.
        int available = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
        if (available < ELEMENT_POINT_LENGTH) {
            source.advance();
        }
    }
}

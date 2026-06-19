/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.util.MutableObjectIterator;

import java.io.Closeable;
import java.io.IOException;

/** Gets two matched rows for full outer join. */
public class SortMergeFullOuterJoinIterator implements Closeable {

    private final Projection<RowData, BinaryRowData> projection1;
    private final Projection<RowData, BinaryRowData> projection2;
    private final RecordComparator keyComparator;
    private final MutableObjectIterator<BinaryRowData> iterator1;
    private final MutableObjectIterator<BinaryRowData> iterator2;

    private BinaryRowData row1;
    private BinaryRowData key1;
    private BinaryRowData row2;
    private BinaryRowData key2;

    private BinaryRowData matchKey;

    private ResettableExternalBuffer buffer1;
    private ResettableExternalBuffer buffer2;

    private final int[] nullFilterKeys;
    private final boolean nullSafe;
    private final boolean filterAllNulls;

    public SortMergeFullOuterJoinIterator(
            BinaryRowDataSerializer serializer1,
            BinaryRowDataSerializer serializer2,
            Projection<RowData, BinaryRowData> projection1,
            Projection<RowData, BinaryRowData> projection2,
            RecordComparator keyComparator,
            MutableObjectIterator<BinaryRowData> iterator1,
            MutableObjectIterator<BinaryRowData> iterator2,
            ResettableExternalBuffer buffer1,
            ResettableExternalBuffer buffer2,
            boolean[] filterNulls)
            throws IOException {
        this.projection1 = projection1;
        this.projection2 = projection2;
        this.keyComparator = keyComparator;
        this.iterator1 = iterator1;
        this.iterator2 = iterator2;

        this.row1 = serializer1.createInstance();
        this.row2 = serializer2.createInstance();
        this.buffer1 = buffer1;
        this.buffer2 = buffer2;
        this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNulls);
        this.nullSafe = nullFilterKeys.length == 0;
        this.filterAllNulls = nullFilterKeys.length == filterNulls.length;

        nextRow1();
        nextRow2();
    }

    private boolean shouldFilter(BinaryRowData key) {
        return NullAwareJoinHelper.shouldFilter(nullSafe, filterAllNulls, nullFilterKeys, key);
    }

    public boolean nextOuterJoin() throws IOException {
        if (key1 != null && (shouldFilter(key1) || key2 == null)) {
            matchKey = null;
            bufferRows1();
            buffer2.reset();
            buffer2.complete();
            return true; // outer row1.
        } else if (key2 != null && (shouldFilter(key2) || key1 == null)) {
            matchKey = null;
            buffer1.reset();
            buffer1.complete();
            bufferRows2();
            return true; // outer row2.
        } else if (key1 != null && key2 != null) {
            int cmp = keyComparator.compare(key1, key2);
            if (cmp == 0) {
                matchKey = key1;
                bufferRows1();
                bufferRows2(); // match join.
            } else if (cmp > 0) {
                matchKey = null;
                buffer1.reset();
                buffer1.complete();
                bufferRows2(); // outer row2.
            } else {
                matchKey = null;
                buffer2.reset();
                buffer2.complete();
                bufferRows1(); // outer row1.
            }
            return true;
        } else {
            return false; // bye bye.
        }
    }

    /** Buffer rows from iterator1 with same key. */
    private void bufferRows1() throws IOException {
        BinaryRowData copy = key1.copy();
        buffer1.reset();
        do {
            buffer1.add(row1);
        } while (nextRow1() && keyComparator.compare(key1, copy) == 0);
        buffer1.complete();
    }

    /** Buffer rows from iterator2 with same key. */
    private void bufferRows2() throws IOException {
        BinaryRowData copy = key2.copy();
        buffer2.reset();
        do {
            buffer2.add(row2);
        } while (nextRow2() && keyComparator.compare(key2, copy) == 0);
        buffer2.complete();
    }

    private boolean nextRow1() throws IOException {
        if ((row1 = iterator1.next(row1)) != null) {
            key1 = projection1.apply(row1);
            return true;
        } else {
            row1 = null;
            key1 = null;
            return false;
        }
    }

    private boolean nextRow2() throws IOException {
        if ((row2 = iterator2.next(row2)) != null) {
            key2 = projection2.apply(row2);
            return true;
        } else {
            row2 = null;
            key2 = null;
            return false;
        }
    }

    public BinaryRowData getMatchKey() {
        return matchKey;
    }

    public ResettableExternalBuffer getBuffer1() {
        return buffer1;
    }

    public ResettableExternalBuffer getBuffer2() {
        return buffer2;
    }

    @Override
    public void close() {
        buffer1.close();
        buffer2.close();
    }
}

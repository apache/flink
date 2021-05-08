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

package org.apache.flink.table.filesystem;

import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.connector.file.src.util.RecyclableIterator;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

/**
 * A {@link BulkFormat.RecordIterator} that returns {@link RowData}s. The next row is set by {@link
 * ColumnarRowData#setRowId}.
 */
public class ColumnarRowIterator extends RecyclableIterator<RowData> {

    private final ColumnarRowData rowData;
    private final MutableRecordAndPosition<RowData> recordAndPosition;

    private int num;
    private int pos;

    public ColumnarRowIterator(ColumnarRowData rowData, @Nullable Runnable recycler) {
        super(recycler);
        this.rowData = rowData;
        this.recordAndPosition = new MutableRecordAndPosition<>();
    }

    /**
     * @param num number rows in this batch.
     * @param recordSkipCount The number of rows that have been returned before this batch.
     */
    public void set(final int num, final long recordSkipCount) {
        set(num, CheckpointedPosition.NO_OFFSET, recordSkipCount);
    }

    /** Set number rows in this batch and updates the position. */
    public void set(final int num, final long offset, final long recordSkipCount) {
        this.num = num;
        this.pos = 0;
        this.recordAndPosition.set(null, offset, recordSkipCount);
    }

    @Nullable
    @Override
    public RecordAndPosition<RowData> next() {
        if (pos < num) {
            rowData.setRowId(pos++);
            recordAndPosition.setNext(rowData);
            return recordAndPosition;
        } else {
            return null;
        }
    }
}

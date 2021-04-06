/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.table.data.vector.writable.WritableDoubleVector;
import org.apache.flink.table.data.vector.writable.WritableIntVector;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Double {@link ColumnReader}. */
public class DoubleColumnReader extends AbstractColumnReader<WritableDoubleVector> {

    public DoubleColumnReader(ColumnDescriptor descriptor, PageReader pageReader)
            throws IOException {
        super(descriptor, pageReader);
        checkTypeName(PrimitiveType.PrimitiveTypeName.DOUBLE);
    }

    @Override
    protected void readBatch(int rowId, int num, WritableDoubleVector column) {
        int left = num;
        while (left > 0) {
            if (runLenDecoder.currentCount == 0) {
                runLenDecoder.readNextGroup();
            }
            int n = Math.min(left, runLenDecoder.currentCount);
            switch (runLenDecoder.mode) {
                case RLE:
                    if (runLenDecoder.currentValue == maxDefLevel) {
                        readDoubles(n, column, rowId);
                    } else {
                        column.setNulls(rowId, n);
                    }
                    break;
                case PACKED:
                    for (int i = 0; i < n; ++i) {
                        if (runLenDecoder.currentBuffer[runLenDecoder.currentBufferIdx++]
                                == maxDefLevel) {
                            column.setDouble(rowId + i, readDouble());
                        } else {
                            column.setNullAt(rowId + i);
                        }
                    }
                    break;
            }
            rowId += n;
            left -= n;
            runLenDecoder.currentCount -= n;
        }
    }

    @Override
    protected void readBatchFromDictionaryIds(
            int rowId, int num, WritableDoubleVector column, WritableIntVector dictionaryIds) {
        for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
                column.setDouble(i, dictionary.decodeToDouble(dictionaryIds.getInt(i)));
            }
        }
    }

    private double readDouble() {
        return readDataBuffer(8).getDouble();
    }

    private void readDoubles(int total, WritableDoubleVector c, int rowId) {
        int requiredBytes = total * 8;
        ByteBuffer buffer = readDataBuffer(requiredBytes);

        if (buffer.hasArray()) {
            int offset = buffer.arrayOffset() + buffer.position();
            c.setDoublesFromBinary(rowId, total, buffer.array(), offset);
        } else {
            for (int i = 0; i < total; i += 1) {
                c.setDouble(rowId + i, buffer.getDouble());
            }
        }
    }
}

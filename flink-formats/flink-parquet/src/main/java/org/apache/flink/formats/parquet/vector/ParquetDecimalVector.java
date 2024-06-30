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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.DecimalColumnVector;
import org.apache.flink.table.data.columnar.vector.Dictionary;
import org.apache.flink.table.data.columnar.vector.IntColumnVector;
import org.apache.flink.table.data.columnar.vector.LongColumnVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableBytesVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableIntVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableLongVector;

import org.apache.parquet.Preconditions;

/**
 * Parquet write decimal as int32 and int64 and binary, this class wrap the real vector to provide
 * {@link DecimalColumnVector} interface.
 */
@Internal
public class ParquetDecimalVector
        implements DecimalColumnVector, WritableLongVector, WritableIntVector, WritableBytesVector {

    private final ColumnVector vector;

    public ParquetDecimalVector(ColumnVector vector) {
        this.vector = vector;
    }

    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
        if (ParquetSchemaConverter.is32BitDecimal(precision) && vector instanceof IntColumnVector) {
            return DecimalData.fromUnscaledLong(
                    ((IntColumnVector) vector).getInt(i), precision, scale);
        } else if (ParquetSchemaConverter.is64BitDecimal(precision)
                && vector instanceof LongColumnVector) {
            return DecimalData.fromUnscaledLong(
                    ((LongColumnVector) vector).getLong(i), precision, scale);
        } else {
            Preconditions.checkArgument(
                    vector instanceof BytesColumnVector,
                    "Reading decimal type occur unsupported vector type: %s",
                    vector.getClass());
            return DecimalData.fromUnscaledBytes(
                    ((BytesColumnVector) vector).getBytes(i).getBytes(), precision, scale);
        }
    }

    public ColumnVector getVector() {
        return vector;
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNullAt(i);
    }

    @Override
    public void reset() {
        if (vector instanceof WritableColumnVector) {
            ((WritableColumnVector) vector).reset();
        }
    }

    @Override
    public void setNullAt(int rowId) {
        if (vector instanceof WritableColumnVector) {
            ((WritableColumnVector) vector).setNullAt(rowId);
        }
    }

    @Override
    public void setNulls(int rowId, int count) {
        if (vector instanceof WritableColumnVector) {
            ((WritableColumnVector) vector).setNulls(rowId, count);
        }
    }

    @Override
    public void fillWithNulls() {
        if (vector instanceof WritableColumnVector) {
            ((WritableColumnVector) vector).fillWithNulls();
        }
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        if (vector instanceof WritableColumnVector) {
            ((WritableColumnVector) vector).setDictionary(dictionary);
        }
    }

    @Override
    public boolean hasDictionary() {
        if (vector instanceof WritableColumnVector) {
            return ((WritableColumnVector) vector).hasDictionary();
        }
        return false;
    }

    @Override
    public WritableIntVector reserveDictionaryIds(int capacity) {
        if (vector instanceof WritableColumnVector) {
            return ((WritableColumnVector) vector).reserveDictionaryIds(capacity);
        }
        throw new RuntimeException("Child vector must be instance of WritableColumnVector");
    }

    @Override
    public WritableIntVector getDictionaryIds() {
        if (vector instanceof WritableColumnVector) {
            return ((WritableColumnVector) vector).getDictionaryIds();
        }
        throw new RuntimeException("Child vector must be instance of WritableColumnVector");
    }

    @Override
    public Bytes getBytes(int i) {
        if (vector instanceof WritableBytesVector) {
            return ((WritableBytesVector) vector).getBytes(i);
        }
        throw new RuntimeException("Child vector must be instance of WritableColumnVector");
    }

    @Override
    public void appendBytes(int rowId, byte[] value, int offset, int length) {
        if (vector instanceof WritableBytesVector) {
            ((WritableBytesVector) vector).appendBytes(rowId, value, offset, length);
        }
    }

    @Override
    public void fill(byte[] value) {
        if (vector instanceof WritableBytesVector) {
            ((WritableBytesVector) vector).fill(value);
        }
    }

    @Override
    public int getInt(int i) {
        if (vector instanceof WritableIntVector) {
            return ((WritableIntVector) vector).getInt(i);
        }
        throw new RuntimeException("Child vector must be instance of WritableColumnVector");
    }

    @Override
    public void setInt(int rowId, int value) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).setInt(rowId, value);
        }
    }

    @Override
    public void setIntsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).setIntsFromBinary(rowId, count, src, srcIndex);
        }
    }

    @Override
    public void setInts(int rowId, int count, int value) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).setInts(rowId, count, value);
        }
    }

    @Override
    public void setInts(int rowId, int count, int[] src, int srcIndex) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).setInts(rowId, count, src, srcIndex);
        }
    }

    @Override
    public void fill(int value) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).fill(value);
        }
    }

    @Override
    public long getLong(int i) {
        if (vector instanceof WritableLongVector) {
            return ((WritableLongVector) vector).getLong(i);
        }
        throw new RuntimeException("Child vector must be instance of WritableColumnVector");
    }

    @Override
    public void setLong(int rowId, long value) {
        if (vector instanceof WritableLongVector) {
            ((WritableLongVector) vector).setLong(rowId, value);
        }
    }

    @Override
    public void setLongsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        if (vector instanceof WritableLongVector) {
            ((WritableLongVector) vector).setLongsFromBinary(rowId, count, src, srcIndex);
        }
    }

    @Override
    public void fill(long value) {
        if (vector instanceof WritableLongVector) {
            ((WritableLongVector) vector).fill(value);
        }
    }
}

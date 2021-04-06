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

package org.apache.flink.table.data;

import org.apache.flink.table.data.binary.TypedSetters;
import org.apache.flink.table.data.vector.ArrayColumnVector;
import org.apache.flink.table.data.vector.BooleanColumnVector;
import org.apache.flink.table.data.vector.ByteColumnVector;
import org.apache.flink.table.data.vector.BytesColumnVector;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.DecimalColumnVector;
import org.apache.flink.table.data.vector.DoubleColumnVector;
import org.apache.flink.table.data.vector.FloatColumnVector;
import org.apache.flink.table.data.vector.IntColumnVector;
import org.apache.flink.table.data.vector.LongColumnVector;
import org.apache.flink.table.data.vector.ShortColumnVector;
import org.apache.flink.table.data.vector.TimestampColumnVector;

import java.util.Arrays;

/** Columnar array to support access to vector column data. */
public final class ColumnarArrayData implements ArrayData, TypedSetters {

    private final ColumnVector data;
    private final int offset;
    private final int numElements;

    public ColumnarArrayData(ColumnVector data, int offset, int numElements) {
        this.data = data;
        this.offset = offset;
        this.numElements = numElements;
    }

    @Override
    public int size() {
        return numElements;
    }

    @Override
    public boolean isNullAt(int pos) {
        return data.isNullAt(offset + pos);
    }

    @Override
    public void setNullAt(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public boolean getBoolean(int pos) {
        return ((BooleanColumnVector) data).getBoolean(offset + pos);
    }

    @Override
    public byte getByte(int pos) {
        return ((ByteColumnVector) data).getByte(offset + pos);
    }

    @Override
    public short getShort(int pos) {
        return ((ShortColumnVector) data).getShort(offset + pos);
    }

    @Override
    public int getInt(int pos) {
        return ((IntColumnVector) data).getInt(offset + pos);
    }

    @Override
    public long getLong(int pos) {
        return ((LongColumnVector) data).getLong(offset + pos);
    }

    @Override
    public float getFloat(int pos) {
        return ((FloatColumnVector) data).getFloat(offset + pos);
    }

    @Override
    public double getDouble(int pos) {
        return ((DoubleColumnVector) data).getDouble(offset + pos);
    }

    @Override
    public StringData getString(int pos) {
        BytesColumnVector.Bytes byteArray = getByteArray(pos);
        return StringData.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return ((DecimalColumnVector) data).getDecimal(offset + pos, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return ((TimestampColumnVector) data).getTimestamp(offset + pos, precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        throw new UnsupportedOperationException("RawValueData is not supported.");
    }

    @Override
    public byte[] getBinary(int pos) {
        BytesColumnVector.Bytes byteArray = getByteArray(pos);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            return Arrays.copyOfRange(byteArray.data, byteArray.offset, byteArray.len);
        }
    }

    @Override
    public ArrayData getArray(int pos) {
        return ((ArrayColumnVector) data).getArray(offset + pos);
    }

    @Override
    public MapData getMap(int pos) {
        throw new UnsupportedOperationException("Map is not supported.");
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        throw new UnsupportedOperationException("Row is not supported.");
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setByte(int pos, byte value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setShort(int pos, short value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setInt(int pos, int value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setLong(int pos, long value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setFloat(int pos, float value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDouble(int pos, double value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDecimal(int pos, DecimalData value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setTimestamp(int pos, TimestampData value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] res = new boolean[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getBoolean(i);
        }
        return res;
    }

    @Override
    public byte[] toByteArray() {
        byte[] res = new byte[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getByte(i);
        }
        return res;
    }

    @Override
    public short[] toShortArray() {
        short[] res = new short[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getShort(i);
        }
        return res;
    }

    @Override
    public int[] toIntArray() {
        int[] res = new int[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getInt(i);
        }
        return res;
    }

    @Override
    public long[] toLongArray() {
        long[] res = new long[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getLong(i);
        }
        return res;
    }

    @Override
    public float[] toFloatArray() {
        float[] res = new float[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getFloat(i);
        }
        return res;
    }

    @Override
    public double[] toDoubleArray() {
        double[] res = new double[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getDouble(i);
        }
        return res;
    }

    private BytesColumnVector.Bytes getByteArray(int pos) {
        return ((BytesColumnVector) data).getBytes(offset + pos);
    }
}

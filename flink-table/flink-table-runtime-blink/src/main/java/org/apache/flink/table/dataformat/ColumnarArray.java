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

package org.apache.flink.table.dataformat;

import org.apache.flink.table.dataformat.vector.ArrayColumnVector;
import org.apache.flink.table.dataformat.vector.BooleanColumnVector;
import org.apache.flink.table.dataformat.vector.ByteColumnVector;
import org.apache.flink.table.dataformat.vector.BytesColumnVector;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.dataformat.vector.DecimalColumnVector;
import org.apache.flink.table.dataformat.vector.DoubleColumnVector;
import org.apache.flink.table.dataformat.vector.FloatColumnVector;
import org.apache.flink.table.dataformat.vector.IntColumnVector;
import org.apache.flink.table.dataformat.vector.LongColumnVector;
import org.apache.flink.table.dataformat.vector.ShortColumnVector;
import org.apache.flink.table.dataformat.vector.TimestampColumnVector;

import java.util.Arrays;

/**
 * Columnar array to support access to vector column data.
 */
public final class ColumnarArray implements BaseArray {

	private final ColumnVector data;
	private final int offset;
	private final int numElements;

	public ColumnarArray(ColumnVector data, int offset, int numElements) {
		this.data = data;
		this.offset = offset;
		this.numElements = numElements;
	}

	@Override
	public int numElements() {
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
	public boolean getBoolean(int ordinal) {
		return ((BooleanColumnVector) data).getBoolean(offset + ordinal);
	}

	@Override
	public byte getByte(int ordinal) {
		return ((ByteColumnVector) data).getByte(offset + ordinal);
	}

	@Override
	public short getShort(int ordinal) {
		return ((ShortColumnVector) data).getShort(offset + ordinal);
	}

	@Override
	public int getInt(int ordinal) {
		return ((IntColumnVector) data).getInt(offset + ordinal);
	}

	@Override
	public long getLong(int ordinal) {
		return ((LongColumnVector) data).getLong(offset + ordinal);
	}

	@Override
	public float getFloat(int ordinal) {
		return ((FloatColumnVector) data).getFloat(offset + ordinal);
	}

	@Override
	public double getDouble(int ordinal) {
		return ((DoubleColumnVector) data).getDouble(offset + ordinal);
	}

	@Override
	public BinaryString getString(int ordinal) {
		BytesColumnVector.Bytes byteArray = getByteArray(ordinal);
		return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
	}

	@Override
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		return ((DecimalColumnVector) data).getDecimal(offset + ordinal, precision, scale);
	}

	@Override
	public SqlTimestamp getTimestamp(int ordinal, int precision) {
		return ((TimestampColumnVector) data).getTimestamp(offset + ordinal, precision);
	}

	@Override
	public <T> BinaryGeneric<T> getGeneric(int ordinal) {
		throw new UnsupportedOperationException("GenericType is not supported.");
	}

	@Override
	public byte[] getBinary(int ordinal) {
		BytesColumnVector.Bytes byteArray = getByteArray(ordinal);
		if (byteArray.len == byteArray.data.length) {
			return byteArray.data;
		} else {
			return Arrays.copyOfRange(byteArray.data, byteArray.offset, byteArray.len);
		}
	}

	@Override
	public BaseArray getArray(int ordinal) {
		return ((ArrayColumnVector) data).getArray(offset + ordinal);
	}

	@Override
	public BaseMap getMap(int ordinal) {
		throw new UnsupportedOperationException("Map is not supported.");
	}

	@Override
	public BaseRow getRow(int ordinal, int numFields) {
		throw new UnsupportedOperationException("Row is not supported.");
	}

	@Override
	public void setBoolean(int ordinal, boolean value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setByte(int ordinal, byte value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setShort(int ordinal, short value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setInt(int ordinal, int value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setLong(int ordinal, long value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setFloat(int ordinal, float value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setDouble(int ordinal, double value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setDecimal(int i, Decimal value, int precision) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setTimestamp(int ordinal, SqlTimestamp value, int precision) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setNotNullAt(int pos) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setNullLong(int pos) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setNullInt(int pos) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setNullBoolean(int pos) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setNullByte(int pos) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setNullShort(int pos) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setNullFloat(int pos) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setNullDouble(int pos) {
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

	private BytesColumnVector.Bytes getByteArray(int ordinal) {
		return ((BytesColumnVector) data).getBytes(offset + ordinal);
	}
}

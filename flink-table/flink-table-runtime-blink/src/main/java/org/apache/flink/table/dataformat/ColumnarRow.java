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

package org.apache.flink.table.dataformat;

import org.apache.flink.table.dataformat.vector.BytesColumnVector.Bytes;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;

/**
 * Columnar row to support access to vector column data. It is a row view in {@link VectorizedColumnBatch}.
 */
public final class ColumnarRow implements BaseRow {
	private byte header;
	private VectorizedColumnBatch vectorizedColumnBatch;
	private int rowId;

	public ColumnarRow() {}

	public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch) {
		this(vectorizedColumnBatch, 0);
	}

	public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch, int rowId) {
		this.vectorizedColumnBatch = vectorizedColumnBatch;
		this.rowId = rowId;
	}

	public void setVectorizedColumnBatch(
			VectorizedColumnBatch vectorizedColumnBatch) {
		this.vectorizedColumnBatch = vectorizedColumnBatch;
		this.rowId = 0;
	}

	public void setRowId(int rowId) {
		this.rowId = rowId;
	}

	@Override
	public byte getHeader() {
		return header;
	}

	@Override
	public void setHeader(byte header) {
		this.header = header;
	}

	@Override
	public int getArity() {
		return vectorizedColumnBatch.getArity();
	}

	@Override
	public boolean isNullAt(int ordinal) {
		return vectorizedColumnBatch.isNullAt(rowId, ordinal);
	}

	@Override
	public boolean getBoolean(int ordinal) {
		return vectorizedColumnBatch.getBoolean(rowId, ordinal);
	}

	@Override
	public byte getByte(int ordinal) {
		return vectorizedColumnBatch.getByte(rowId, ordinal);
	}

	@Override
	public short getShort(int ordinal) {
		return vectorizedColumnBatch.getShort(rowId, ordinal);
	}

	@Override
	public int getInt(int ordinal) {
		return vectorizedColumnBatch.getInt(rowId, ordinal);
	}

	@Override
	public long getLong(int ordinal) {
		return vectorizedColumnBatch.getLong(rowId, ordinal);
	}

	@Override
	public float getFloat(int ordinal) {
		return vectorizedColumnBatch.getFloat(rowId, ordinal);
	}

	@Override
	public double getDouble(int ordinal) {
		return vectorizedColumnBatch.getDouble(rowId, ordinal);
	}

	@Override
	public BinaryString getString(int ordinal) {
		Bytes byteArray = vectorizedColumnBatch.getByteArray(rowId, ordinal);
		return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
	}

	@Override
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		return vectorizedColumnBatch.getDecimal(rowId, ordinal, precision, scale);
	}

	@Override
	public <T> BinaryGeneric<T> getGeneric(int pos) {
		throw new UnsupportedOperationException("GenericType is not supported.");
	}

	@Override
	public byte[] getBinary(int ordinal) {
		Bytes byteArray = vectorizedColumnBatch.getByteArray(rowId, ordinal);
		if (byteArray.len == byteArray.data.length) {
			return byteArray.data;
		} else {
			byte[] ret = new byte[byteArray.len];
			System.arraycopy(byteArray.data, byteArray.offset, ret, 0, byteArray.len);
			return ret;
		}
	}

	@Override
	public BaseRow getRow(int ordinal, int numFields) {
		// TODO
		throw new UnsupportedOperationException("Row is not supported.");
	}

	@Override
	public BaseArray getArray(int ordinal) {
		// TODO
		throw new UnsupportedOperationException("Array is not supported.");
	}

	@Override
	public BaseMap getMap(int ordinal) {
		// TODO
		throw new UnsupportedOperationException("Map is not supported.");
	}

	@Override
	public void setNullAt(int ordinal) {
		throw new UnsupportedOperationException("Not support the operation!");
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
	public void setFloat(int pos, float value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setDouble(int ordinal, double value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setDecimal(int ordinal, Decimal value, int precision) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public boolean equals(Object o) {
		throw new UnsupportedOperationException(
				"ColumnarRow do not support equals, please compare fields one by one!");
	}

	@Override
	public int hashCode() {
		throw new UnsupportedOperationException(
				"ColumnarRow do not support hashCode, please hash fields one by one!");
	}
}

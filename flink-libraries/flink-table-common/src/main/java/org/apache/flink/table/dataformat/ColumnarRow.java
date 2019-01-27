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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.runtime.util.StringUtf8Utils;

/**
 * Source directly return columnRow in order to reduce Row convert times.
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
	public char getChar(int ordinal) {
		throw new UnsupportedOperationException("char is not supported.");
	}

	@Override
	public byte[] getByteArray(int ordinal) {
		VectorizedColumnBatch.ByteArray byteArray = vectorizedColumnBatch.getByteArray(rowId, ordinal);
		if (byteArray.len == byteArray.data.length) {
			return byteArray.data;
		} else {
			byte[] ret = new byte[byteArray.len];
			System.arraycopy(byteArray.data, byteArray.offset, ret, 0, byteArray.len);
			return ret;
		}
	}

	@Override
	public BinaryString getBinaryString(int ordinal) {
		VectorizedColumnBatch.ByteArray byteArray = vectorizedColumnBatch.getByteArray(rowId, ordinal);
		MemorySegment memorySegment = MemorySegmentFactory.wrap(byteArray.data);
		return BinaryString.fromAddress(new MemorySegment[]{memorySegment}, byteArray.offset, byteArray.len);
	}

	@Override
	public BinaryString getBinaryString(int ordinal, BinaryString reuse) {
		VectorizedColumnBatch.ByteArray byteArray = vectorizedColumnBatch.getByteArray(rowId, ordinal);
		reuse.pointTo(byteArray.data, byteArray.offset, byteArray.len);
		return reuse;
	}

	@Override
	public String getString(int ordinal) {
		VectorizedColumnBatch.ByteArray byteArray = vectorizedColumnBatch.getByteArray(rowId, ordinal);
		return StringUtf8Utils.decodeUTF8(byteArray.data, byteArray.offset, byteArray.len);
	}

	@Override
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		return vectorizedColumnBatch.getDecimal(rowId, ordinal);
	}

	@Override
	public <T> T getGeneric(int pos, TypeSerializer<T> serializer) {
		throw new UnsupportedOperationException(serializer.toString() + " is not supported.");
	}

	@Override
	public <T> T getGeneric(int pos, GenericType<T> type) {
		throw new UnsupportedOperationException(type.toString() + " is not supported.");
	}

	@Override
	public BaseRow getBaseRow(int ordinal, int numFields) {
		throw new UnsupportedOperationException();
	}

	@Override
	public BaseArray getBaseArray(int ordinal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public BaseMap getBaseMap(int ordinal) {
		throw new UnsupportedOperationException();
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
	public void setChar(int ordinal, char value) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	@Override
	public void setDecimal(int ordinal, Decimal value, int precision, int scale) {
		throw new UnsupportedOperationException("Not support the operation!");
	}

	private Object internalObject(int ordinal) {
		return vectorizedColumnBatch.getInternalObject(rowId, ordinal);
	}

	@Override
	public boolean equalsWithoutHeader(BaseRow other) {
		if (this == other) {
			return true;
		}
		if (other == null || !(other instanceof ColumnarRow)) {
			return false;
		}

		ColumnarRow row = (ColumnarRow) other;
		for (int i = 0; i < row.getArity(); i++) {
			if (internalObject(i).equals(row.internalObject(i))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean equals(Object other) {
		return other != null && other instanceof BaseRow &&
				equalsWithoutHeader((BaseRow) other) &&
				getHeader() == ((BaseRow) other).getHeader();
	}
}

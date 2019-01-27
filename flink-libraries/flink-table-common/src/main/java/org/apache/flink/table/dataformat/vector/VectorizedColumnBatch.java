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

package org.apache.flink.table.dataformat.vector;

import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.Types;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.util.TimeConvertUtils;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * A VectorizedColumnBatch is a set of rows, organized with each column
 * as a vector. It is the unit of query execution, organized to minimize
 * the cost per row and achieve high cycles-per-instruction.
 * The major fields are public by design to allow fast and convenient
 * access by the vectorized query execution code.
 */
public class VectorizedColumnBatch implements Serializable {
	private static final long serialVersionUID = 8180323238728166155L;

	/**
	 * This number is carefully chosen to minimize overhead and typically allows
	 * one VectorizedColumnBatch to fit in cache.
	 */
	public static final int MAX_SIZE = 2048;

	protected InternalType[] fieldTypes;
	private final int capacity;
	private int numRows;
	public final ColumnVector[] columns;
	private final boolean copyToInternal;

	public static VectorizedColumnBatch allocate(InternalType[] fieldTypes) {
		return new VectorizedColumnBatch(fieldTypes, MAX_SIZE);
	}

	/**
	 * Return a batch with the specified number of columns and rows.
	 * Only call this constructor directly for testing purposes.
	 * Batch size should normally always be defaultSize.
	 *
	 */
	private VectorizedColumnBatch(InternalType[] fieldTypes, int maxRows) {
		this.fieldTypes = fieldTypes;
		this.capacity = maxRows;
		this.columns = new ColumnVector[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			createColumn(this.columns, i, fieldTypes[i], maxRows);
		}
		this.copyToInternal = true;
	}

	public VectorizedColumnBatch(InternalType[] fieldTypes, int maxRows, ColumnVector[] columns) {
		this.fieldTypes = fieldTypes;
		this.capacity = maxRows;
		this.columns = columns;
		this.copyToInternal = false;
	}

	private void createColumn(ColumnVector[] columns, int index, InternalType fieldType, int maxRows) {
		if (fieldType.equals(Types.BOOLEAN)) {
			columns[index] = new BooleanColumnVector(maxRows);
		} else if (fieldType.equals(Types.BYTE)) {
			columns[index] = new ByteColumnVector(maxRows);
		} else if (fieldType.equals(Types.DOUBLE)) {
			columns[index] = new DoubleColumnVector(maxRows);
		} else if (fieldType.equals(Types.FLOAT)) {
			columns[index] = new FloatColumnVector(maxRows);
		} else if (fieldType.equals(Types.INT) ||
				(fieldType instanceof DecimalType && Decimal.is32BitDecimal(((DecimalType) fieldType).precision()))) {
			columns[index] = new IntegerColumnVector(maxRows);
		} else if (fieldType.equals(Types.LONG) ||
				(fieldType instanceof DecimalType && Decimal.is64BitDecimal(((DecimalType) fieldType).precision()))) {
			columns[index] = new LongColumnVector(maxRows);
		} else if (fieldType.equals(Types.SHORT)) {
			columns[index] = new ShortColumnVector(maxRows);
		} else if (fieldType.equals(Types.STRING)) {
			columns[index] = new StringColumnVector(maxRows);
		} else if (fieldType.equals(Types.BYTE_ARRAY) ||
				(fieldType instanceof DecimalType && Decimal.isByteArrayDecimal(((DecimalType) fieldType).precision()))) {
			columns[index] = new BytesColumnVector(maxRows);
		} else if (fieldType.equals(Types.DATE)) {
			columns[index] = new DateColumnVector(maxRows);
		} else if (fieldType.equals(Types.TIME)) {
			columns[index] = new TimeColumnVector(maxRows);
		} else if (fieldType.equals(Types.TIMESTAMP)) {
			columns[index] = new TimestampColumnVector(maxRows);
		} else {
			throw new UnsupportedOperationException(fieldType  + " is not supported now.");
		}
	}

	public void close() {

	}

	/**
	 * Resets the batch for writing.
	 */
	public void reset() {
		for (ColumnVector column : columns) {
			column.reset();
		}
		this.numRows = 0;
	}

	public int capacity() {
		return this.capacity;
	}

	public void setNumRows(int numRows) {
		this.numRows = numRows;
	}

	public int getNumRows() {
		return numRows;
	}

	public int getArity() {
		return columns.length;
	}

	public boolean isNullAt(int rowId, int colId) {
		return !columns[colId].noNulls && columns[colId].isNull[rowId];
	}

	public boolean getBoolean(int rowId, int colId) {
		if (!copyToInternal) {
			return ((TypeGetVector) columns[colId]).getBoolean(rowId);
		}

		ColumnVector columnVector = columns[colId];
		if (columnVector.dictionary == null) {
			BooleanColumnVector booleanColumnVector = (BooleanColumnVector) columnVector;
			return booleanColumnVector.vector[rowId];
		} else {
			return columnVector.dictionary.decodeToBoolean(columnVector.dictionaryIds.vector[rowId]);
		}
	}

	public byte getByte(int rowId, int colId) {
		if (!copyToInternal) {
			return ((TypeGetVector) columns[colId]).getByte(rowId);
		}

		ColumnVector columnVector = columns[colId];
		if (columnVector.dictionary == null) {
			ByteColumnVector byteColumnVector = (ByteColumnVector) columnVector;
			return byteColumnVector.vector[rowId];
		} else {
			return (byte) columnVector.dictionary.decodeToInt(columnVector.dictionaryIds.vector[rowId]);
		}
	}

	public short getShort(int rowId, int colId) {
		if (!copyToInternal) {
			return ((TypeGetVector) columns[colId]).getShort(rowId);
		}

		ColumnVector columnVector = columns[colId];
		if (columnVector.dictionary == null) {
			ShortColumnVector shortColumnVector = (ShortColumnVector) columnVector;
			return shortColumnVector.vector[rowId];
		} else {
			return (short) columnVector.dictionary.decodeToInt(columnVector.dictionaryIds.vector[rowId]);
		}
	}

	public int getInt(int rowId, int colId) {
		if (!copyToInternal) {
			return ((TypeGetVector) columns[colId]).getInt(rowId);
		}

		ColumnVector columnVector = columns[colId];
		if (columnVector.dictionary == null) {
			IntegerColumnVector integerColumnVector = (IntegerColumnVector) columns[colId];
			return integerColumnVector.vector[rowId];
		} else {
			return columnVector.dictionary.decodeToInt(columnVector.dictionaryIds.vector[rowId]);
		}
	}

	public long getLong(int rowId, int colId) {
		if (!copyToInternal) {
			return ((TypeGetVector) columns[colId]).getLong(rowId);
		}

		ColumnVector columnVector = columns[colId];
		if (columnVector.dictionary == null) {
			LongColumnVector longColumnVector = (LongColumnVector) columns[colId];
			return longColumnVector.vector[rowId];
		} else {
			return columnVector.dictionary.decodeToLong(columnVector.dictionaryIds.vector[rowId]);
		}
	}

	public float getFloat(int rowId, int colId) {
		if (!copyToInternal) {
			return ((TypeGetVector) columns[colId]).getFloat(rowId);
		}
		ColumnVector columnVector = columns[colId];
		if (columns[colId].dictionary == null) {
			FloatColumnVector floatColumnVector = (FloatColumnVector) columns[colId];
			return floatColumnVector.vector[rowId];
		} else {
			return columnVector.dictionary.decodeToFloat(columnVector.dictionaryIds.vector[rowId]);
		}
	}

	public double getDouble(int rowId, int colId) {
		if (!copyToInternal) {
			return ((TypeGetVector) columns[colId]).getDouble(rowId);
		}

		ColumnVector columnVector = columns[colId];
		if (columns[colId].dictionary == null) {
			DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columns[colId];
			return doubleColumnVector.vector[rowId];
		} else {
			return columnVector.dictionary.decodeToDouble(columnVector.dictionaryIds.vector[rowId]);
		}
	}

	public ByteArray getByteArray(int rowId, int colId) {
		if (!copyToInternal) {
			return ((TypeGetVector) columns[colId]).getByteArray(rowId);
		}

		ColumnVector columnVector = columns[colId];
		if (columns[colId].dictionary == null) {
			BytesColumnVector bytesColumnVector = (BytesColumnVector) columns[colId];
			return new ByteArray(bytesColumnVector.buffer,
				bytesColumnVector.start[rowId],
				bytesColumnVector.length[rowId]);
		} else {
			byte[] bytes = columnVector.dictionary.decodeToBinary(columnVector.dictionaryIds.vector[rowId]);
			return new ByteArray(bytes, 0, bytes.length);
		}
	}

	private byte[] getBytes(int rowId, int colId) {
		ByteArray byteArray = getByteArray(rowId, colId);
		if (byteArray.len == byteArray.data.length) {
			return byteArray.data;
		} else {
			return byteArray.getBytes();
		}
	}

	public String getString(int rowId, int colId) {
		ByteArray byteArray = getByteArray(rowId, colId);
		return new String(byteArray.data, byteArray.offset, byteArray.len);
	}

	public Date getDate(int rowId, int colId) {
		return TimeConvertUtils.internalToDate(getInt(rowId, colId));
	}

	public Time getTime(int rowId, int colId) {
		return TimeConvertUtils.internalToTime(getInt(rowId, colId));
	}

	public Timestamp getTimestamp(int rowId, int colId) {
		return TimeConvertUtils.internalToTimestamp(getLong(rowId, colId));
	}

	public Decimal getDecimal(int rowId, int colId) {
		if (isNullAt(rowId, colId)) {
			return null;
		}
		DecimalType decimalTypeInfo = (DecimalType) fieldTypes[colId];
		int precision = decimalTypeInfo.precision();
		int scale = decimalTypeInfo.scale();

		if (!copyToInternal) {
			return ((TypeGetVector) columns[colId]).getDecimal(rowId, precision, scale);
		}

		if (Decimal.is32BitDecimal(precision)) {
			return Decimal.fromUnscaledLong(precision, scale, getInt(rowId, colId));
		} else if (Decimal.is64BitDecimal(precision)) {
			return Decimal.fromUnscaledLong(precision, scale, getLong(rowId, colId));
		} else {
			byte[] bytes = getBytes(rowId, colId);
			return Decimal.fromUnscaledBytes(precision, scale, bytes);
		}
	}

	public Object getInternalObject(int rowId, int colId) {
		if (!columns[colId].noNulls && columns[colId].isNull[rowId]) {
			return null;
		} else if (Types.INT.equals(fieldTypes[colId])) {
			return getInt(rowId, colId);
		} else if (Types.SHORT.equals(fieldTypes[colId])) {
			return getShort(rowId, colId);
		} else if (Types.BOOLEAN.equals(fieldTypes[colId])) {
			return getBoolean(rowId, colId);
		} else if (Types.BYTE.equals(fieldTypes[colId])) {
			return getByte(rowId, colId);
		} else if (Types.DOUBLE.equals(fieldTypes[colId])) {
			return getDouble(rowId, colId);
		} else if (Types.FLOAT.equals(fieldTypes[colId])) {
			return getFloat(rowId, colId);
		} else if (Types.LONG.equals(fieldTypes[colId])) {
			return getLong(rowId, colId);
		} else if (Types.STRING.equals(fieldTypes[colId])) {
			return BinaryString.fromBytes(getBytes(rowId, colId));
		} else if (Types.TIMESTAMP.equals(fieldTypes[colId])) {
			return getLong(rowId, colId);
		} else if (Types.DATE.equals(fieldTypes[colId])) {
			return getInt(rowId, colId);
		} else if (Types.TIME.equals(fieldTypes[colId])) {
			return getInt(rowId, colId);
		} else if (Types.BYTE_ARRAY.equals(fieldTypes[colId])) {
			return getBytes(rowId, colId);
		} else if (fieldTypes[colId] instanceof DecimalType) {
			return getDecimal(rowId, colId);
		} else {
			throw new RuntimeException(fieldTypes[colId] + " is not supported.");
		}
	}

	public Object getObject(int rowId, int colId) {
		if (!columns[colId].noNulls && columns[colId].isNull[rowId]) {
			return null;
		} else if (Types.INT.equals(fieldTypes[colId])) {
			return getInt(rowId, colId);
		} else if (Types.SHORT.equals(fieldTypes[colId])) {
			return getShort(rowId, colId);
		} else if (Types.BOOLEAN.equals(fieldTypes[colId])) {
			return getBoolean(rowId, colId);
		} else if (Types.BYTE.equals(fieldTypes[colId])) {
			return getByte(rowId, colId);
		} else if (Types.DOUBLE.equals(fieldTypes[colId])) {
			return getDouble(rowId, colId);
		} else if (Types.FLOAT.equals(fieldTypes[colId])) {
			return getFloat(rowId, colId);
		} else if (Types.LONG.equals(fieldTypes[colId])) {
			return getLong(rowId, colId);
		} else if (Types.STRING.equals(fieldTypes[colId])) {
			return getString(rowId, colId);
		} else if (Types.TIMESTAMP.equals(fieldTypes[colId])) {
			return getTimestamp(rowId, colId);
		} else if (Types.DATE.equals(fieldTypes[colId])) {
			return getDate(rowId, colId);
		} else if (Types.TIME.equals(fieldTypes[colId])) {
			return getTime(rowId, colId);
		} else if (Types.BYTE_ARRAY.equals(fieldTypes[colId])) {
			return getBytes(rowId, colId);
		} else if (fieldTypes[colId] instanceof DecimalType) {
			return getDecimal(rowId, colId);
		} else {
			throw new RuntimeException(fieldTypes[colId] + " is not supported.");
		}
	}

	/**
	 * For nested byte array.
	 */
	public static class ByteArray{
		public final byte[] data;
		public final int offset;
		public final int len;

		public ByteArray(byte[] data, int offset, int len) {
			this.data = data;
			this.offset = offset;
			this.len = len;
		}

		public byte[] getBytes() {
			byte[] res = new byte[len];
			System.arraycopy(data, offset, res, 0, len);
			return res;
		}
	}
}

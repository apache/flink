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

import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.table.dataformat.vector.BytesColumnVector.Bytes;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * A VectorizedColumnBatch is a set of rows, organized with each column as a vector. It is the
 * unit of query execution, organized to minimize the cost per row.
 *
 * <p>{@code VectorizedColumnBatch}s are influenced by Apache Hive VectorizedRowBatch.
 */
public class VectorizedColumnBatch implements Serializable {
	private static final long serialVersionUID = 8180323238728166155L;

	/**
	 * This number is carefully chosen to minimize overhead and typically allows
	 * one VectorizedColumnBatch to fit in cache.
	 */
	public static final int DEFAULT_SIZE = 2048;

	private int numRows;
	public final ColumnVector[] columns;

	public VectorizedColumnBatch(ColumnVector[] vectors) {
		this.columns = vectors;
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
		return columns[colId].isNullAt(rowId);
	}

	public boolean getBoolean(int rowId, int colId) {
		return ((BooleanColumnVector) columns[colId]).getBoolean(rowId);
	}

	public byte getByte(int rowId, int colId) {
		return ((ByteColumnVector) columns[colId]).getByte(rowId);
	}

	public short getShort(int rowId, int colId) {
		return ((ShortColumnVector) columns[colId]).getShort(rowId);
	}

	public int getInt(int rowId, int colId) {
		return ((IntColumnVector) columns[colId]).getInt(rowId);
	}

	public long getLong(int rowId, int colId) {
		return ((LongColumnVector) columns[colId]).getLong(rowId);
	}

	public float getFloat(int rowId, int colId) {
		return ((FloatColumnVector) columns[colId]).getFloat(rowId);
	}

	public double getDouble(int rowId, int colId) {
		return ((DoubleColumnVector) columns[colId]).getDouble(rowId);
	}

	public Bytes getByteArray(int rowId, int colId) {
		return ((BytesColumnVector) columns[colId]).getBytes(rowId);
	}

	private byte[] getBytes(int rowId, int colId) {
		Bytes byteArray = getByteArray(rowId, colId);
		if (byteArray.len == byteArray.data.length) {
			return byteArray.data;
		} else {
			return byteArray.getBytes();
		}
	}

	public String getString(int rowId, int colId) {
		Bytes byteArray = getByteArray(rowId, colId);
		return new String(byteArray.data, byteArray.offset, byteArray.len, StandardCharsets.UTF_8);
	}

	public Decimal getDecimal(int rowId, int colId, int precision, int scale) {
		return ((DecimalColumnVector) (columns[colId])).getDecimal(rowId, precision, scale);
	}

	public SqlTimestamp getTimestamp(int rowId, int colId, int precision) {
		return ((TimestampColumnVector) (columns[colId])).getTimestamp(rowId, precision);
	}
}

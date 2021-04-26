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

package org.apache.flink.table.data;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.RowKind;

import java.util.Objects;

/**
 * An implementation of {@link RowData} which is backed by two concatenated {@link RowData}.
 */
@Internal
public class JoinedRowData implements RowData {

	private RowKind rowKind = RowKind.INSERT;
	private RowData row1;
	private RowData row2;

	public JoinedRowData() {}

	public JoinedRowData(RowData row1, RowData row2) {
		this.row1 = row1;
		this.row2 = row2;
	}

	public JoinedRowData replace(RowData row1, RowData row2) {
		this.row1 = row1;
		this.row2 = row2;
		return this;
	}

	@Override
	public int getArity() {
		return row1.getArity() + row2.getArity();
	}

	@Override
	public RowKind getRowKind() {
		return rowKind;
	}

	@Override
	public void setRowKind(RowKind kind) {
		this.rowKind = kind;
	}

	@Override
	public boolean isNullAt(int pos) {
		if (pos < row1.getArity()) {
			return row1.isNullAt(pos);
		} else {
			return row2.isNullAt(pos - row1.getArity());
		}
	}

	@Override
	public boolean getBoolean(int pos) {
		if (pos < row1.getArity()) {
			return row1.getBoolean(pos);
		} else {
			return row2.getBoolean(pos - row1.getArity());
		}
	}

	@Override
	public byte getByte(int pos) {
		if (pos < row1.getArity()) {
			return row1.getByte(pos);
		} else {
			return row2.getByte(pos - row1.getArity());
		}
	}

	@Override
	public short getShort(int pos) {
		if (pos < row1.getArity()) {
			return row1.getShort(pos);
		} else {
			return row2.getShort(pos - row1.getArity());
		}
	}

	@Override
	public int getInt(int pos) {
		if (pos < row1.getArity()) {
			return row1.getInt(pos);
		} else {
			return row2.getInt(pos - row1.getArity());
		}
	}

	@Override
	public long getLong(int pos) {
		if (pos < row1.getArity()) {
			return row1.getLong(pos);
		} else {
			return row2.getLong(pos - row1.getArity());
		}
	}

	@Override
	public float getFloat(int pos) {
		if (pos < row1.getArity()) {
			return row1.getFloat(pos);
		} else {
			return row2.getFloat(pos - row1.getArity());
		}
	}

	@Override
	public double getDouble(int pos) {
		if (pos < row1.getArity()) {
			return row1.getDouble(pos);
		} else {
			return row2.getDouble(pos - row1.getArity());
		}
	}

	@Override
	public StringData getString(int pos) {
		if (pos < row1.getArity()) {
			return row1.getString(pos);
		} else {
			return row2.getString(pos - row1.getArity());
		}
	}

	@Override
	public DecimalData getDecimal(int pos, int precision, int scale) {
		if (pos < row1.getArity()) {
			return row1.getDecimal(pos, precision, scale);
		} else {
			return row2.getDecimal(pos - row1.getArity(), precision, scale);
		}
	}

	@Override
	public TimestampData getTimestamp(int pos, int precision) {
		if (pos < row1.getArity()) {
			return row1.getTimestamp(pos, precision);
		} else {
			return row2.getTimestamp(pos - row1.getArity(), precision);
		}
	}

	@Override
	public <T> RawValueData<T> getRawValue(int pos) {
		if (pos < row1.getArity()) {
			return row1.getRawValue(pos);
		} else {
			return row2.getRawValue(pos - row1.getArity());
		}
	}

	@Override
	public byte[] getBinary(int pos) {
		if (pos < row1.getArity()) {
			return row1.getBinary(pos);
		} else {
			return row2.getBinary(pos - row1.getArity());
		}
	}

	@Override
	public ArrayData getArray(int pos) {
		if (pos < row1.getArity()) {
			return row1.getArray(pos);
		} else {
			return row2.getArray(pos - row1.getArity());
		}
	}

	@Override
	public MapData getMap(int pos) {
		if (pos < row1.getArity()) {
			return row1.getMap(pos);
		} else {
			return row2.getMap(pos - row1.getArity());
		}
	}

	@Override
	public RowData getRow(int pos, int numFields) {
		if (pos < row1.getArity()) {
			return row1.getRow(pos, numFields);
		} else {
			return row2.getRow(pos - row1.getArity(), numFields);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JoinedRowData that = (JoinedRowData) o;
		return Objects.equals(rowKind, that.rowKind) &&
			Objects.equals(this.row1, that.row1) &&
			Objects.equals(this.row2, that.row2);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rowKind, row1, row2);
	}

	@Override
	public String toString() {
		return rowKind.shortString() + "{" +
			"row1=" + row1 +
			", row2=" + row2 +
			'}';
	}
}

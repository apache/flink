/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.types.GenericType;

/**
 * Join two row to one row.
 */
public final class JoinedRow implements BaseRow {

	private BaseRow row1;
	private BaseRow row2;
	private byte header;

	public JoinedRow() {}

	public JoinedRow(BaseRow row1, BaseRow row2) {
		this.row1 = row1;
		this.row2 = row2;
	}

	public JoinedRow replace(BaseRow row1, BaseRow row2) {
		this.row1 = row1;
		this.row2 = row2;
		return this;
	}

	@Override
	public int getArity() {
		return row1.getArity() + row2.getArity();
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
	public boolean isNullAt(int i) {
		if (i < row1.getArity()) {
			return row1.isNullAt(i);
		} else {
			return row2.isNullAt(i - row1.getArity());
		}
	}

	@Override
	public boolean getBoolean(int i) {
		if (i < row1.getArity()) {
			return row1.getBoolean(i);
		} else {
			return row2.getBoolean(i - row1.getArity());
		}
	}

	@Override
	public byte getByte(int i) {
		if (i < row1.getArity()) {
			return row1.getByte(i);
		} else {
			return row2.getByte(i - row1.getArity());
		}
	}

	@Override
	public short getShort(int i) {
		if (i < row1.getArity()) {
			return row1.getShort(i);
		} else {
			return row2.getShort(i - row1.getArity());
		}
	}

	@Override
	public int getInt(int i) {
		if (i < row1.getArity()) {
			return row1.getInt(i);
		} else {
			return row2.getInt(i - row1.getArity());
		}
	}

	@Override
	public long getLong(int i) {
		if (i < row1.getArity()) {
			return row1.getLong(i);
		} else {
			return row2.getLong(i - row1.getArity());
		}
	}

	@Override
	public float getFloat(int i) {
		if (i < row1.getArity()) {
			return row1.getFloat(i);
		} else {
			return row2.getFloat(i - row1.getArity());
		}
	}

	@Override
	public double getDouble(int i) {
		if (i < row1.getArity()) {
			return row1.getDouble(i);
		} else {
			return row2.getDouble(i - row1.getArity());
		}
	}

	@Override
	public char getChar(int i) {
		if (i < row1.getArity()) {
			return row1.getChar(i);
		} else {
			return row2.getChar(i - row1.getArity());
		}
	}

	@Override
	public byte[] getByteArray(int i) {
		if (i < row1.getArity()) {
			return row1.getByteArray(i);
		} else {
			return row2.getByteArray(i - row1.getArity());
		}
	}

	@Override
	public BinaryString getBinaryString(int i) {
		if (i < row1.getArity()) {
			return row1.getBinaryString(i);
		} else {
			return row2.getBinaryString(i - row1.getArity());
		}
	}

	@Override
	public BinaryString getBinaryString(int i, BinaryString reuse) {
		if (i < row1.getArity()) {
			return row1.getBinaryString(i, reuse);
		} else {
			return row2.getBinaryString(i - row1.getArity(), reuse);
		}
	}

	@Override
	public Decimal getDecimal(int i, int precision, int scale) {
		if (i < row1.getArity()) {
			return row1.getDecimal(i, precision, scale);
		} else {
			return row2.getDecimal(i - row1.getArity(), precision, scale);
		}
	}

	@Override
	public String getString(int i) {
		if (i < row1.getArity()) {
			return row1.getString(i);
		} else {
			return row2.getString(i - row1.getArity());
		}
	}

	@Override
	public <T> T getGeneric(int i, TypeSerializer<T> serializer) {
		if (i < row1.getArity()) {
			return row1.getGeneric(i, serializer);
		} else {
			return row2.getGeneric(i - row1.getArity(), serializer);
		}
	}

	@Override
	public <T> T getGeneric(int i, GenericType<T> type) {
		if (i < row1.getArity()) {
			return row1.getGeneric(i, type);
		} else {
			return row2.getGeneric(i - row1.getArity(), type);
		}
	}

	@Override
	public BaseRow getBaseRow(int i, int numFields) {
		if (i < row1.getArity()) {
			return row1.getBaseRow(i, numFields);
		} else {
			return row2.getBaseRow(i - row1.getArity(), numFields);
		}
	}

	@Override
	public BaseArray getBaseArray(int i) {
		if (i < row1.getArity()) {
			return row1.getBaseArray(i);
		} else {
			return row2.getBaseArray(i - row1.getArity());
		}
	}

	@Override
	public BaseMap getBaseMap(int i) {
		if (i < row1.getArity()) {
			return row1.getBaseMap(i);
		} else {
			return row2.getBaseMap(i - row1.getArity());
		}
	}

	@Override
	public void setNullAt(int i) {
		if (i < row1.getArity()) {
			row1.setNullAt(i);
		} else {
			row2.setNullAt(i - row1.getArity());
		}
	}

	@Override
	public void setBoolean(int i, boolean value) {
		if (i < row1.getArity()) {
			row1.setBoolean(i, value);
		} else {
			row2.setBoolean(i - row1.getArity(), value);
		}
	}

	@Override
	public void setByte(int i, byte value) {
		if (i < row1.getArity()) {
			row1.setByte(i, value);
		} else {
			row2.setByte(i - row1.getArity(), value);
		}
	}

	@Override
	public void setShort(int i, short value) {
		if (i < row1.getArity()) {
			row1.setShort(i, value);
		} else {
			row2.setShort(i - row1.getArity(), value);
		}
	}

	@Override
	public void setInt(int i, int value) {
		if (i < row1.getArity()) {
			row1.setInt(i, value);
		} else {
			row2.setInt(i - row1.getArity(), value);
		}
	}

	@Override
	public void setLong(int i, long value) {
		if (i < row1.getArity()) {
			row1.setLong(i, value);
		} else {
			row2.setLong(i - row1.getArity(), value);
		}
	}

	@Override
	public void setFloat(int i, float value) {
		if (i < row1.getArity()) {
			row1.setFloat(i, value);
		} else {
			row2.setFloat(i - row1.getArity(), value);
		}
	}

	@Override
	public void setDouble(int i, double value) {
		if (i < row1.getArity()) {
			row1.setDouble(i, value);
		} else {
			row2.setDouble(i - row1.getArity(), value);
		}
	}

	@Override
	public void setChar(int i, char value) {
		if (i < row1.getArity()) {
			row1.setChar(i, value);
		} else {
			row2.setChar(i - row1.getArity(), value);
		}
	}

	@Override
	public void setDecimal(int i, Decimal value, int precision, int scale) {
		if (i < row1.getArity()) {
			row1.setDecimal(i, value, precision, scale);
		} else {
			row2.setDecimal(i - row1.getArity(), value, precision, scale);
		}
	}

	@Override
	public boolean equalsWithoutHeader(BaseRow o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof JoinedRow)) {
			return false;
		}

		JoinedRow row = (JoinedRow) o;

		return row1.equalsWithoutHeader(row.row1) && row2.equalsWithoutHeader(row.row2);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof JoinedRow)) {
			return false;
		}

		JoinedRow row = (JoinedRow) o;

		return this.header == row.header &&
			row1.equalsWithoutHeader(row.row1) &&
			row2.equalsWithoutHeader(row.row2);
	}

	@Override
	public int hashCode() {
		int res = 31 * Byte.hashCode(header) + row1.hashCode();
		return 31 * res + row2.hashCode();
	}
}

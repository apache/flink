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

import org.apache.flink.table.data.binary.TypedSetters;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.ShortValue;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;

/**
 * An implementation of {@link RowData} which also is also backed by an array of
 * Java {@link Object}, just similar to {@link GenericRowData}.
 * But {@link BoxedWrapperRowData} wraps primitive types into boxed object to avoid
 * boxing and unboxing.
 */
public class BoxedWrapperRowData implements RowData, TypedSetters {

	private RowKind rowKind = RowKind.INSERT; // INSERT as default

	protected final Object[] fields;

	public BoxedWrapperRowData(int arity) {
		this.fields = new Object[arity];
	}

	@Override
	public int getArity() {
		return fields.length;
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
		return this.fields[pos] == null;
	}

	@Override
	public boolean getBoolean(int pos) {
		return ((BooleanValue) fields[pos]).getValue();
	}

	@Override
	public byte getByte(int pos) {
		return ((ByteValue) fields[pos]).getValue();
	}

	@Override
	public short getShort(int pos) {
		return ((ShortValue) fields[pos]).getValue();
	}

	@Override
	public int getInt(int pos) {
		return ((IntValue) fields[pos]).getValue();
	}

	@Override
	public long getLong(int pos) {
		return ((LongValue) fields[pos]).getValue();
	}

	@Override
	public float getFloat(int pos) {
		return ((FloatValue) fields[pos]).getValue();
	}

	@Override
	public double getDouble(int pos) {
		return ((DoubleValue) fields[pos]).getValue();
	}

	@Override
	public StringData getString(int pos) {
		return (StringData) this.fields[pos];
	}

	@Override
	public DecimalData getDecimal(int pos, int precision, int scale) {
		return (DecimalData) this.fields[pos];
	}

	@Override
	public TimestampData getTimestamp(int pos, int precision) {
		return (TimestampData) this.fields[pos];
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> RawValueData<T> getRawValue(int pos) {
		return (RawValueData<T>) this.fields[pos];
	}

	@Override
	public byte[] getBinary(int pos) {
		return (byte[]) this.fields[pos];
	}

	@Override
	public ArrayData getArray(int pos) {
		return (ArrayData) this.fields[pos];
	}

	@Override
	public MapData getMap(int pos) {
		return (MapData) this.fields[pos];
	}

	@Override
	public RowData getRow(int pos, int numFields) {
		return (RowData) this.fields[pos];
	}

	@Override
	public void setNullAt(int pos) {
		this.fields[pos] = null;
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		BooleanValue wrap;
		if ((wrap = (BooleanValue) fields[pos]) == null) {
			wrap = new BooleanValue();
			fields[pos] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setByte(int pos, byte value) {
		ByteValue wrap;
		if ((wrap = (ByteValue) fields[pos]) == null) {
			wrap = new ByteValue();
			fields[pos] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setShort(int pos, short value) {
		ShortValue wrap;
		if ((wrap = (ShortValue) fields[pos]) == null) {
			wrap = new ShortValue();
			fields[pos] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setInt(int pos, int value) {
		IntValue wrap;
		if ((wrap = (IntValue) fields[pos]) == null) {
			wrap = new IntValue();
			fields[pos] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setLong(int pos, long value) {
		LongValue wrap;
		if ((wrap = (LongValue) fields[pos]) == null) {
			wrap = new LongValue();
			fields[pos] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setFloat(int pos, float value) {
		FloatValue wrap;
		if ((wrap = (FloatValue) fields[pos]) == null) {
			wrap = new FloatValue();
			fields[pos] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setDouble(int pos, double value) {
		DoubleValue wrap;
		if ((wrap = (DoubleValue) fields[pos]) == null) {
			wrap = new DoubleValue();
			fields[pos] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setDecimal(int pos, DecimalData value, int precision) {
		this.fields[pos] = value;
	}

	@Override
	public void setTimestamp(int pos, TimestampData value, int precision) {
		this.fields[pos] = value;
	}

	public void setNonPrimitiveValue(int pos, Object value) {
		this.fields[pos] = value;
	}

	@Override
	public int hashCode() {
		return 31 * rowKind.hashCode() + Arrays.hashCode(fields);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BoxedWrapperRowData) {
			BoxedWrapperRowData other = (BoxedWrapperRowData) o;
			return rowKind == other.rowKind && Arrays.equals(fields, other.fields);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(rowKind.shortString()).append("(");
		for (int i = 0; i < fields.length; i++) {
			if (i != 0) {
				sb.append(",");
			}
			sb.append(StringUtils.arrayAwareToString(fields[i]));
		}
		sb.append(")");
		return sb.toString();
	}
}

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

package org.apache.flink.table.data;

import org.apache.flink.table.data.binary.TypedSetters;
import org.apache.flink.types.RowKind;

/**
 * An implementation of {@link RowData} which is backed by a {@link RowData} and an updated
 * Java object array.
 */
public final class UpdatableRowData implements RowData, TypedSetters {

	private RowData row;
	private final Object[] fields;
	private final boolean[] updated;

	public UpdatableRowData(RowData row, int arity) {
		this.row = row;
		this.fields = new Object[arity];
		this.updated = new boolean[arity];
	}

	public RowData getRow() {
		return row;
	}

	@Override
	public int getArity() {
		return fields.length;
	}

	@Override
	public RowKind getRowKind() {
		return row.getRowKind();
	}

	@Override
	public void setRowKind(RowKind kind) {
		row.setRowKind(kind);
	}

	@Override
	public boolean isNullAt(int pos) {
		return updated[pos] ? this.fields[pos] == null : row.isNullAt(pos);
	}

	@Override
	public boolean getBoolean(int pos) {
		return updated[pos] ? (boolean) fields[pos] : row.getBoolean(pos);
	}

	@Override
	public byte getByte(int pos) {
		return updated[pos] ? (byte) fields[pos] : row.getByte(pos);
	}

	@Override
	public short getShort(int pos) {
		return updated[pos] ? (short) fields[pos] : row.getShort(pos);
	}

	@Override
	public int getInt(int pos) {
		return updated[pos] ? (int) fields[pos] : row.getInt(pos);
	}

	@Override
	public long getLong(int pos) {
		return updated[pos] ? (long) fields[pos] : row.getLong(pos);
	}

	@Override
	public float getFloat(int pos) {
		return updated[pos] ? (float) fields[pos] : row.getFloat(pos);
	}

	@Override
	public double getDouble(int pos) {
		return updated[pos] ? (double) fields[pos] : row.getDouble(pos);
	}

	@Override
	public byte[] getBinary(int pos) {
		return updated[pos] ? (byte[]) fields[pos] : row.getBinary(pos);
	}

	@Override
	public StringData getString(int pos) {
		return updated[pos] ? (StringData) fields[pos] : row.getString(pos);
	}

	@Override
	public DecimalData getDecimal(int pos, int precision, int scale) {
		return updated[pos] ? (DecimalData) fields[pos] : row.getDecimal(pos, precision, scale);
	}

	@Override
	public TimestampData getTimestamp(int pos, int precision) {
		return updated[pos] ? (TimestampData) fields[pos] : row.getTimestamp(pos, precision);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> RawValueData<T> getRawValue(int pos) {
		return updated[pos] ? (RawValueData<T>) fields[pos] : row.getRawValue(pos);
	}

	@Override
	public RowData getRow(int pos, int numFields) {
		return updated[pos] ? (RowData) fields[pos] : row.getRow(pos, numFields);
	}

	@Override
	public ArrayData getArray(int pos) {
		return updated[pos] ? (ArrayData) fields[pos] : row.getArray(pos);
	}

	@Override
	public MapData getMap(int pos) {
		return updated[pos] ? (MapData) fields[pos] : row.getMap(pos);
	}

	@Override
	public void setNullAt(int pos) {
		setField(pos, null);
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		setField(pos, value);
	}

	@Override
	public void setByte(int pos, byte value) {
		setField(pos, value);
	}

	@Override
	public void setShort(int pos, short value) {
		setField(pos, value);
	}

	@Override
	public void setInt(int pos, int value) {
		setField(pos, value);
	}

	@Override
	public void setLong(int pos, long value) {
		setField(pos, value);
	}

	@Override
	public void setFloat(int pos, float value) {
		setField(pos, value);
	}

	@Override
	public void setDouble(int pos, double value) {
		setField(pos, value);
	}

	@Override
	public void setDecimal(int pos, DecimalData value, int precision) {
		setField(pos, value);
	}

	@Override
	public void setTimestamp(int pos, TimestampData value, int precision) {
		setField(pos, value);
	}

	public void setField(int pos, Object value) {
		updated[pos] = true;
		fields[pos] = value;
	}
}


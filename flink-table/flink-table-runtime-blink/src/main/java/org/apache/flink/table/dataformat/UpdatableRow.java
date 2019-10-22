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

/**
 * Wrap row to a updatable Generic Row.
 */
public final class UpdatableRow implements BaseRow {

	private BaseRow row;
	private final Object[] fields;
	private final boolean[] updated;

	public UpdatableRow(BaseRow row, int arity) {
		this.row = row;
		this.fields = new Object[arity];
		this.updated = new boolean[arity];
	}

	public BaseRow getRow() {
		return row;
	}

	@Override
	public int getArity() {
		return fields.length;
	}

	@Override
	public byte getHeader() {
		return row.getHeader();
	}

	@Override
	public void setHeader(byte header) {
		row.setHeader(header);
	}

	@Override
	public boolean isNullAt(int ordinal) {
		return updated[ordinal] ? this.fields[ordinal] == null : row.isNullAt(ordinal);
	}

	@Override
	public boolean getBoolean(int ordinal) {
		return updated[ordinal] ? (boolean) fields[ordinal] : row.getBoolean(ordinal);
	}

	@Override
	public byte getByte(int ordinal) {
		return updated[ordinal] ? (byte) fields[ordinal] : row.getByte(ordinal);
	}

	@Override
	public short getShort(int ordinal) {
		return updated[ordinal] ? (short) fields[ordinal] : row.getShort(ordinal);
	}

	@Override
	public int getInt(int ordinal) {
		return updated[ordinal] ? (int) fields[ordinal] : row.getInt(ordinal);
	}

	@Override
	public long getLong(int ordinal) {
		return updated[ordinal] ? (long) fields[ordinal] : row.getLong(ordinal);
	}

	@Override
	public float getFloat(int ordinal) {
		return updated[ordinal] ? (float) fields[ordinal] : row.getFloat(ordinal);
	}

	@Override
	public double getDouble(int ordinal) {
		return updated[ordinal] ? (double) fields[ordinal] : row.getDouble(ordinal);
	}

	@Override
	public byte[] getBinary(int ordinal) {
		return updated[ordinal] ? (byte[]) fields[ordinal] : row.getBinary(ordinal);
	}

	@Override
	public BinaryString getString(int ordinal) {
		return updated[ordinal] ? (BinaryString) fields[ordinal] : row.getString(ordinal);
	}

	@Override
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		return updated[ordinal] ? (Decimal) fields[ordinal] : row.getDecimal(ordinal, precision, scale);
	}

	@Override
	public <T> BinaryGeneric<T> getGeneric(int ordinal) {
		return updated[ordinal] ? (BinaryGeneric<T>) fields[ordinal] : row.getGeneric(ordinal);
	}

	@Override
	public BaseRow getRow(int ordinal, int numFields) {
		return updated[ordinal] ? (BaseRow) fields[ordinal] : row.getRow(ordinal, numFields);
	}

	@Override
	public BaseArray getArray(int ordinal) {
		return updated[ordinal] ? (BaseArray) fields[ordinal] : row.getArray(ordinal);
	}

	@Override
	public BaseMap getMap(int ordinal) {
		return updated[ordinal] ? (BinaryMap) fields[ordinal] : row.getMap(ordinal);
	}

	@Override
	public void setNullAt(int ordinal) {
		setField(ordinal, null);
	}

	@Override
	public void setBoolean(int ordinal, boolean value) {
		setField(ordinal, value);
	}

	@Override
	public void setByte(int ordinal, byte value) {
		setField(ordinal, value);
	}

	@Override
	public void setShort(int ordinal, short value) {
		setField(ordinal, value);
	}

	@Override
	public void setInt(int ordinal, int value) {
		setField(ordinal, value);
	}

	@Override
	public void setLong(int ordinal, long value) {
		setField(ordinal, value);
	}

	@Override
	public void setFloat(int ordinal, float value) {
		setField(ordinal, value);
	}

	@Override
	public void setDouble(int ordinal, double value) {
		setField(ordinal, value);
	}

	@Override
	public void setDecimal(int ordinal, Decimal value, int precision) {
		setField(ordinal, value);
	}

	public void setField(int ordinal, Object value) {
		updated[ordinal] = true;
		fields[ordinal] = value;
	}
}


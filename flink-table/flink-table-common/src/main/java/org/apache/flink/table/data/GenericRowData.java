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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.RowKind;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link GenericRowData} can have arbitrary number of fields and contain a set of fields,
 * which may all be different types. The fields in {@link GenericRowData} can be null.
 *
 * <p>The fields in the row can be accessed by position (zero-based) {@link #getInt},
 * and can be updated by {@link #setField(int, Object)}. All the fields should be in
 * internal data structures.
 *
 * @see RowData for more information about the mapping of Table/SQL data types
 * 				and internal data structures.
 */
@PublicEvolving
public final class GenericRowData implements RowData {

	/** The array to store the actual internal format values. */
	private final Object[] fields;

	/** The changelog kind of this row. */
	private RowKind kind;

	/**
	 * Create a new GenericRow instance.
	 * @param arity The number of fields in the GenericRow
	 */
	public GenericRowData(int arity) {
		this.fields = new Object[arity];
		this.kind = RowKind.INSERT; // INSERT as default
	}

	/**
	 * Sets the field at the specified ordinal.
	 *
	 * <p>Note: the given field value must in internal data structures, otherwise the
	 * {@link GenericRowData} is corrupted, and may throw exception when processing.
	 * See the description of {@link RowData} for more information about internal data structures.
	 *
	 * @param ordinal The ordinal of the field, 0-based.
	 * @param value The internal data value to be assigned to the field at the specified ordinal.
	 * @throws IndexOutOfBoundsException if the ordinal is negative, or equal to, or larger than
	 * 									 the number of fields.
	 */
	public void setField(int ordinal, Object value) {
		this.fields[ordinal] = value;
	}

	/**
	 * Gets the field at the specified ordinal.
	 *
	 * <p>Note: the returned value is in internal data structure.
	 * See the description of {@link RowData} for more information about internal data structures.
	 *
	 * @param ordinal The ordinal of the field, 0-based.
	 * @return The field at the specified position.
	 * @throws IndexOutOfBoundsException if the ordinal is negative, or equal to, or larger than
	 * 									 the number of fields.
	 */
	public Object getField(int ordinal) {
		return this.fields[ordinal];
	}

	@Override
	public int getArity() {
		return fields.length;
	}

	@Override
	public RowKind getRowKind() {
		return kind;
	}

	@Override
	public void setRowKind(RowKind kind) {
		checkNotNull(kind);
		this.kind = kind;
	}

	@Override
	public boolean isNullAt(int ordinal) {
		return this.fields[ordinal] == null;
	}

	@Override
	public boolean getBoolean(int ordinal) {
		return (boolean) this.fields[ordinal];
	}

	@Override
	public byte getByte(int ordinal) {
		return (byte) this.fields[ordinal];
	}

	@Override
	public short getShort(int ordinal) {
		return (short) this.fields[ordinal];
	}

	@Override
	public int getInt(int ordinal) {
		return (int) this.fields[ordinal];
	}

	@Override
	public long getLong(int ordinal) {
		return (long) this.fields[ordinal];
	}

	@Override
	public float getFloat(int ordinal) {
		return (float) this.fields[ordinal];
	}

	@Override
	public double getDouble(int ordinal) {
		return (double) this.fields[ordinal];
	}

	@Override
	public StringData getString(int ordinal) {
		return (StringData) this.fields[ordinal];
	}

	@Override
	public DecimalData getDecimal(int ordinal, int precision, int scale) {
		return (DecimalData) this.fields[ordinal];
	}

	@Override
	public TimestampData getTimestamp(int ordinal, int precision) {
		return (TimestampData) this.fields[ordinal];
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> RawValueData<T> getRawValue(int ordinal) {
		return (RawValueData<T>) this.fields[ordinal];
	}

	@Override
	public byte[] getBinary(int ordinal) {
		return (byte[]) this.fields[ordinal];
	}

	@Override
	public ArrayData getArray(int ordinal) {
		return (ArrayData) this.fields[ordinal];
	}

	@Override
	public MapData getMap(int ordinal) {
		return (MapData) this.fields[ordinal];
	}

	@Override
	public RowData getRow(int ordinal, int numFields) {
		return (RowData) this.fields[ordinal];
	}

	// ----------------------------------------------------------------------------------------
	// Utilities
	// ----------------------------------------------------------------------------------------

	/**
	 * Creates a GenericRow with the given internal format values and a default
	 * {@link RowKind#INSERT} change kind.
	 *
	 * @param values internal format values
	 */
	public static GenericRowData of(Object... values) {
		GenericRowData row = new GenericRowData(values.length);

		for (int i = 0; i < values.length; ++i) {
			row.setField(i, values[i]);
		}

		return row;
	}
}

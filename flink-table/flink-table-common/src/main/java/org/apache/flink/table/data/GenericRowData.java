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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An internal data structure representing data of {@link RowType} and other (possibly nested) structured
 * types such as {@link StructuredType}.
 *
 * <p>{@link GenericRowData} is a generic implementation of {@link RowData} which is backed by an
 * array of Java {@link Object}. A {@link GenericRowData} can have an arbitrary number of fields of
 * different types. The fields in a row can be accessed by position (0-based) using either the generic
 * {@link #getField(int)} or type-specific getters (such as {@link #getInt(int)}). A field can be updated
 * by the generic {@link #setField(int, Object)}.
 *
 * <p>Note: All fields of this data structure must be internal data structures. See {@link RowData} for
 * more information about internal data structures.
 *
 * <p>The fields in {@link GenericRowData} can be null for representing nullability.
 */
@PublicEvolving
public final class GenericRowData implements RowData {

	/** The array to store the actual internal format values. */
	private final Object[] fields;

	/** The kind of change that a row describes in a changelog. */
	private RowKind kind;

	/**
	 * Creates an instance of {@link GenericRowData} with given number of fields.
	 *
	 * <p>Initially, all fields are set to null. By default, the row describes a {@link RowKind#INSERT}
	 * in a changelog.
	 *
	 * <p>Note: All fields of the row must be internal data structures.
	 *
	 * @param arity number of fields
	 */
	public GenericRowData(int arity) {
		this.fields = new Object[arity];
		this.kind = RowKind.INSERT; // INSERT as default
	}

	/**
	 * Sets the field value at the given position.
	 *
	 * <p>Note: The given field value must be an internal data structures. Otherwise the {@link GenericRowData}
	 * is corrupted and may throw exception when processing. See {@link RowData} for more information
	 * about internal data structures.
	 *
	 * <p>The field value can be null for representing nullability.
	 */
	public void setField(int pos, Object value) {
		this.fields[pos] = value;
	}

	/**
	 * Returns the field value at the given position.
	 *
	 * <p>Note: The returned value is in internal data structure. See {@link RowData} for more information
	 * about internal data structures.
	 *
	 * <p>The returned field value can be null for representing nullability.
	 */
	public Object getField(int pos) {
		return this.fields[pos];
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
	public boolean isNullAt(int pos) {
		return this.fields[pos] == null;
	}

	@Override
	public boolean getBoolean(int pos) {
		return (boolean) this.fields[pos];
	}

	@Override
	public byte getByte(int pos) {
		return (byte) this.fields[pos];
	}

	@Override
	public short getShort(int pos) {
		return (short) this.fields[pos];
	}

	@Override
	public int getInt(int pos) {
		return (int) this.fields[pos];
	}

	@Override
	public long getLong(int pos) {
		return (long) this.fields[pos];
	}

	@Override
	public float getFloat(int pos) {
		return (float) this.fields[pos];
	}

	@Override
	public double getDouble(int pos) {
		return (double) this.fields[pos];
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
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof GenericRowData)) {
			return false;
		}
		GenericRowData that = (GenericRowData) o;
		return kind == that.kind &&
			Arrays.deepEquals(fields, that.fields);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(kind);
		result = 31 * result + Arrays.deepHashCode(fields);
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(kind.shortString()).append("(");
		for (int i = 0; i < fields.length; i++) {
			if (i != 0) {
				sb.append(",");
			}
			sb.append(StringUtils.arrayAwareToString(fields[i]));
		}
		sb.append(")");
		return sb.toString();
	}

	// ----------------------------------------------------------------------------------------
	// Utilities
	// ----------------------------------------------------------------------------------------

	/**
	 * Creates an instance of {@link GenericRowData} with given field values.
	 *
	 * <p>By default, the row describes a {@link RowKind#INSERT} in a changelog.
	 *
	 * <p>Note: All fields of the row must be internal data structures.
	 */
	public static GenericRowData of(Object... values) {
		GenericRowData row = new GenericRowData(values.length);

		for (int i = 0; i < values.length; ++i) {
			row.setField(i, values[i]);
		}

		return row;
	}
}

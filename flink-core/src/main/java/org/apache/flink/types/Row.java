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
package org.apache.flink.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A Row can have arbitrary number of fields and contain a set of fields, which may all be
 * different types. The fields in Row can be null. Due to Row is not strongly typed, Flink's
 * type extraction mechanism can't extract correct field types. So that users should manually
 * tell Flink the type information via creating a {@link RowTypeInfo}.
 *
 * <p>
 * The fields in the Row can be accessed by position (zero-based) {@link #getField(int)}. And can
 * set fields by {@link #setField(int, Object)}.
 * <p>
 * Row is in principle serializable. However, it may contain non-serializable fields,
 * in which case serialization will fail.
 *
 */
@PublicEvolving
public class Row implements Serializable{

	private static final long serialVersionUID = 1L;

	/** The array to store actual values. */
	private final Object[] fields;

	/**
	 * Create a new Row instance.
	 * @param arity The number of fields in the Row
	 */
	public Row(int arity) {
		this.fields = new Object[arity];
	}

	/**
	 * Get the number of fields in the Row.
	 * @return The number of fields in the Row.
	 */
	public int getArity() {
		return fields.length;
	}

	/**
	 * Gets the field at the specified position.
	 * @param pos The position of the field, 0-based.
	 * @return The field at the specified position.
	 * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
	 */
	public Object getField(int pos) {
		return fields[pos];
	}

	/**
	 * Sets the field at the specified position.
	 *
	 * @param pos The position of the field, 0-based.
	 * @param value The value to be assigned to the field at the specified position.
	 * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
	 */
	public void setField(int pos, Object value) {
		fields[pos] = value;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fields.length; i++) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append(StringUtils.arrayAwareToString(fields[i]));
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Row row = (Row) o;

		return Arrays.deepEquals(fields, row.fields);
	}

	@Override
	public int hashCode() {
		return Arrays.deepHashCode(fields);
	}

	/**
	 * Creates a new Row and assigns the given values to the Row's fields.
	 * This is more convenient than using the constructor.
	 *
	 * <p>For example:
	 *
	 * <pre>
	 *     Row.of("hello", true, 1L);}
	 * </pre>
	 * instead of
	 * <pre>
	 *     Row row = new Row(3);
	 *     row.setField(0, "hello");
	 *     row.setField(1, true);
	 *     row.setField(2, 1L);
	 * </pre>
	 *
	 */
	public static Row of(Object... values) {
		Row row = new Row(values.length);
		for (int i = 0; i < values.length; i++) {
			row.setField(i, values[i]);
		}
		return row;
	}

	/**
	 * Creates a new Row which copied from another row.
	 * This method does not perform a deep copy.
	 *
	 * @param row The row being copied.
	 * @return The cloned new Row
	 */
	public static Row copy(Row row) {
		final Row newRow = new Row(row.fields.length);
		System.arraycopy(row.fields, 0, newRow.fields, 0, row.fields.length);
		return newRow;
	}

	/**
	 * Creates a new Row with projected fields from another row.
	 * This method does not perform a deep copy.
	 *
	 * @param fields fields to be projected
	 * @return the new projected Row
	 */
	public static Row project(Row row, int[] fields) {
		final Row newRow = new Row(fields.length);
		for (int i = 0; i < fields.length; i++) {
			newRow.fields[i] = row.fields[fields[i]];
		}
		return newRow;
	}
}

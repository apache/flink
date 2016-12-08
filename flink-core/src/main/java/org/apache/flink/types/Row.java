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

import java.io.Serializable;
import java.util.Arrays;

/**
 * A Row has no limited length and contain a set of fields, which may all be different types.
 * Because Row is not strongly typed, Flink's type extraction mechanism can't extract correct field
 * types. So that users should manually tell Flink the type information via creating a
 * {@link RowTypeInfo}.
 *
 * <p>
 * The fields in the Row may be accessed by position (zero-based) {@link #getField(int)}. And can
 * set fields by {@link #setField(int, Object)}.
 * <p>
 * Row is in principle serializable. However, it may contain non-serializable fields,
 * in which case serialization will fail.
 *
 */
@PublicEvolving
public class Row implements Serializable{

	private static final long serialVersionUID = 1L;

	/** Number of field. */
	private final int arity;
	/** The array to store actual values. */
	private final Object[] fields;

	/**
	 * Create a new Row instance.
	 * @param arity The number of field in the Row
	 */
	public Row(int arity) {
		this.arity = arity;
		this.fields = new Object[arity];
	}

	/**
	 * Get the number of field in the Row.
	 * @return The number of field in the Row.
	 */
	public int getArity() {
		return arity;
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
		return Arrays.deepToString(fields);
	}

	/**
	 * Deep equality for Row by calling equals on each field.
	 * @param o The object checked for equality.
	 * @return true if this is equal to o.
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Row row = (Row) o;

		return arity == row.arity && Arrays.equals(fields, row.fields);
	}

	@Override
	public int hashCode() {
		int result = arity;
		result = 31 * result + Arrays.hashCode(fields);
		return result;
	}
}

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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.types.RowUtils.deepEqualsRow;
import static org.apache.flink.types.RowUtils.deepHashCodeRow;

/**
 * A row is a fixed-length, null-aware composite type for storing multiple values in a deterministic
 * field order. Every field can be null regardless of the field's type. The type of row fields cannot
 * be automatically inferred; therefore, it is required to provide type information whenever a row is
 * produced.
 *
 * <p>The main purpose of rows is to bridge between Flink's Table and SQL ecosystem and other APIs. Therefore,
 * a row does not only consist of a schema part (containing the fields) but also attaches a {@link RowKind}
 * for encoding a change in a changelog. Thus, a row can be considered as an entry in a changelog. For example,
 * in regular batch scenarios, a changelog would consist of a bounded stream of {@link RowKind#INSERT} rows.
 *
 * <p>The fields of a row can be accessed by position (zero-based) using {@link #getField(int)} and
 * {@link #setField(int, Object)}. The row kind is kept separate from the fields and can be accessed
 * by using {@link #getKind()} and {@link #setKind(RowKind)}.
 *
 * <p>A row instance is in principle {@link Serializable}. However, it may contain non-serializable fields
 * in which case serialization will fail if the row is not serialized with Flink's serialization stack.
 *
 * <p>The {@link #equals(Object)} and {@link #hashCode()} methods of this class support all external
 * conversion classes of the table ecosystem.
 */
@PublicEvolving
public final class Row implements Serializable {

	private static final long serialVersionUID = 2L;

	/** The kind of change a row describes in a changelog. */
	private RowKind kind;

	/** The array to store actual values. */
	private final Object[] fields;

	/**
	 * Create a new row instance.
	 *
	 * <p>By default, a row describes an {@link RowKind#INSERT} change.
	 *
	 * @param kind kind of change a row describes in a changelog
	 * @param arity The number of fields in the row.
	 */
	public Row(RowKind kind, int arity) {
		this.kind = Preconditions.checkNotNull(kind, "Row kind must not be null.");
		this.fields = new Object[arity];
	}

	/**
	 * Create a new row instance.
	 *
	 * <p>By default, a row describes an {@link RowKind#INSERT} change.
	 *
	 * @param arity The number of fields in the row.
	 */
	public Row(int arity) {
		this(RowKind.INSERT, arity);
	}

	/**
	 * Returns the kind of change that this row describes in a changelog.
	 *
	 * <p>By default, a row describes an {@link RowKind#INSERT} change.
	 *
	 * @see RowKind
	 */
	public RowKind getKind() {
		return kind;
	}

	/**
	 * Sets the kind of change that this row describes in a changelog.
	 *
	 * <p>By default, a row describes an {@link RowKind#INSERT} change.
	 *
	 * @see RowKind
	 */
	public void setKind(RowKind kind) {
		Preconditions.checkNotNull(kind, "Row kind must not be null.");
		this.kind = kind;
	}

	/**
	 * Returns the number of fields in the row.
	 *
	 * <p>Note: The row kind is kept separate from the fields and is not included in this number.
	 *
	 * @return The number of fields in the row.
	 */
	public int getArity() {
		return fields.length;
	}

	/**
	 * Returns the field's content at the specified position.
	 *
	 * @param pos The position of the field, 0-based.
	 * @return The field's content at the specified position.
	 */
	public @Nullable Object getField(int pos) {
		return fields[pos];
	}

	/**
	 * Sets the field's content at the specified position.
	 *
	 * @param pos The position of the field, 0-based.
	 * @param value The value to be assigned to the field at the specified position.
	 */
	public void setField(int pos, @Nullable Object value) {
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
		final Row row = (Row) o;
		return deepEqualsRow(this, row);
	}

	@Override
	public int hashCode() {
		return deepHashCodeRow(this);
	}

	// --------------------------------------------------------------------------------------------
	// Utility methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new row and assigns the given values to the row's fields.
	 * This is more convenient than using the constructor.
	 *
	 * <p>For example:
	 * <pre>
	 *     Row.of("hello", true, 1L);
	 * </pre>
	 * instead of
	 * <pre>
	 *     Row row = new Row(3);
	 *     row.setField(0, "hello");
	 *     row.setField(1, true);
	 *     row.setField(2, 1L);
	 * </pre>
	 *
	 * <p>By default, a row describes an {@link RowKind#INSERT} change.
	 */
	public static Row of(Object... values) {
		Row row = new Row(values.length);
		for (int i = 0; i < values.length; i++) {
			row.setField(i, values[i]);
		}
		return row;
	}

	/**
	 * Creates a new row with given kind and assigns the given values to the row's fields.
	 * This is more convenient than using the constructor.
	 *
	 * <p>For example:
	 * <pre>
	 *     Row.ofKind(RowKind.INSERT, "hello", true, 1L);
	 * </pre>
	 * instead of
	 * <pre>
	 *     Row row = new Row(3);
	 *     row.setKind(RowKind.INSERT);
	 *     row.setField(0, "hello");
	 *     row.setField(1, true);
	 *     row.setField(2, 1L);
	 * </pre>
	 */
	public static Row ofKind(RowKind kind, Object... values) {
		Row row = new Row(kind, values.length);
		for (int i = 0; i < values.length; i++) {
			row.setField(i, values[i]);
		}
		return row;
	}

	/**
	 * Creates a new row which is copied from another row (including its {@link RowKind}).
	 *
	 * <p>This method does not perform a deep copy.
	 */
	public static Row copy(Row row) {
		final Row newRow = new Row(row.kind, row.fields.length);
		System.arraycopy(row.fields, 0, newRow.fields, 0, row.fields.length);
		return newRow;
	}

	/**
	 * Creates a new row with projected fields and identical {@link RowKind} from another row.
	 *
	 * <p>This method does not perform a deep copy.
	 *
	 * @param fields field indices to be projected
	 */
	public static Row project(Row row, int[] fields) {
		final Row newRow = new Row(row.kind, fields.length);
		for (int i = 0; i < fields.length; i++) {
			newRow.fields[i] = row.fields[fields[i]];
		}
		return newRow;
	}

	/**
	 * Creates a new row with fields that are copied from the other rows and appended to the resulting
	 * row in the given order. The {@link RowKind} of the first row determines the {@link RowKind} of
	 * the result.
	 *
	 * <p>This method does not perform a deep copy.
	 */
	public static Row join(Row first, Row... remainings) {
		int newLength = first.fields.length;
		for (Row remaining : remainings) {
			newLength += remaining.fields.length;
		}

		final Row joinedRow = new Row(first.kind, newLength);
		int index = 0;

		// copy the first row
		System.arraycopy(first.fields, 0, joinedRow.fields, index, first.fields.length);
		index += first.fields.length;

		// copy the remaining rows
		for (Row remaining : remainings) {
			System.arraycopy(remaining.fields, 0, joinedRow.fields, index, remaining.fields.length);
			index += remaining.fields.length;
		}

		return joinedRow;
	}
}

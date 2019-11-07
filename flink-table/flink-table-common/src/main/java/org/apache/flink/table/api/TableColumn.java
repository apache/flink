/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * A table column represents a table column's structure with
 * column name, column data type and computation expression(if it is a computed column).
 */
@PublicEvolving
public class TableColumn {

	//~ Instance fields --------------------------------------------------------

	private final String name;
	private final DataType type;
	@Nullable
	private final String expr;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a {@link TableColumn} instance.
	 *
	 * @param name Column name
	 * @param type Column data type
	 * @param expr Column computation expression if it is a computed column
	 */
	private TableColumn(
			String name,
			DataType type,
			@Nullable String expr) {
		this.name = name;
		this.type = type;
		this.expr = expr;
	}

	//~ Methods ----------------------------------------------------------------

	/**
	 * Creates a table column from given name and data type.
	 */
	public static TableColumn of(String name, DataType type) {
		Preconditions.checkNotNull(name, "Column name can not be null!");
		Preconditions.checkNotNull(type, "Column type can not be null!");
		return new TableColumn(name, type, null);
	}

	/**
	 * Creates a table column from given name and computation expression.
	 *
	 * @param name Name of the column
	 * @param expression SQL-style expression
	 */
	public static TableColumn of(String name, DataType type, String expression) {
		Preconditions.checkNotNull(name, "Column name can not be null!");
		Preconditions.checkNotNull(type, "Column type can not be null!");
		Preconditions.checkNotNull(expression, "Column expression can not be null!");
		return new TableColumn(name, type, expression);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableColumn that = (TableColumn) o;
		return Objects.equals(this.name, that.name)
			&& Objects.equals(this.type, that.type)
			&& Objects.equals(this.expr, that.expr);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.name, this.type, this.expr);
	}

	//~ Getter/Setter ----------------------------------------------------------

	/** Returns data type of this column. */
	public DataType getType() {
		return this.type;
	}

	/** Returns name of this column. */
	public String getName() {
		return name;
	}

	/** Returns computation expression of this column. Or empty if this column
	 * is not a computed column. */
	public Optional<String> getExpr() {
		return Optional.ofNullable(this.expr);
	}

	/**
	 * Returns if this column is a computed column that is generated from an expression.
	 *
	 * @return true if this column is generated
	 */
	public boolean isGenerated() {
		return this.expr != null;
	}

}

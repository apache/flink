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

package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;

/**
 * ALTER DDL to CHANGE a column for a table.
 */
public class SqlChangeColumn extends SqlAlterTable {

	private final SqlIdentifier oldName;
	private final SqlTableColumn newColumn;

	// Specify the position of the new column. If neither after nor first is set, the column is changed in place.
	// the name of the column after which the new column should be placed
	private final SqlIdentifier after;
	// whether the new column should be placed as the first column of the schema
	private final boolean first;

	// properties that should be added to the table
	private final SqlNodeList properties;

	public SqlChangeColumn(
			SqlParserPos pos,
			SqlIdentifier tableName,
			SqlIdentifier oldName,
			SqlTableColumn newColumn,
			@Nullable SqlIdentifier after,
			boolean first,
			@Nullable SqlNodeList properties) {
		super(pos, tableName);
		if (after != null && first) {
			throw new IllegalArgumentException("FIRST and AFTER cannot be set at the same time");
		}
		this.oldName = oldName;
		this.newColumn = newColumn;
		this.after = after;
		this.first = first;
		this.properties = properties;
	}

	public SqlIdentifier getOldName() {
		return oldName;
	}

	public SqlTableColumn getNewColumn() {
		return newColumn;
	}

	public SqlIdentifier getAfter() {
		return after;
	}

	public boolean isFirst() {
		return first;
	}

	public SqlNodeList getProperties() {
		return properties;
	}

	@Nonnull
	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(tableIdentifier, partitionSpec, oldName, newColumn, after);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		super.unparse(writer, leftPrec, rightPrec);
		writer.keyword("CHANGE COLUMN");
		oldName.unparse(writer, leftPrec, rightPrec);
		newColumn.unparse(writer, leftPrec, rightPrec);
		if (first) {
			writer.keyword("FIST");
		}
		if (after != null) {
			writer.keyword("AFTER");
			after.unparse(writer, leftPrec, rightPrec);
		}
	}
}

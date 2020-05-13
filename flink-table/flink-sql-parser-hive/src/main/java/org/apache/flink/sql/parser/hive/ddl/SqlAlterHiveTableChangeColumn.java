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

package org.apache.flink.sql.parser.hive.ddl;

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.hive.impl.ParseException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_COLUMN;

/**
 * ALTER DDL to change a column's name, type, position, etc.
 */
public class SqlAlterHiveTableChangeColumn extends SqlAlterHiveTableColumn {

	public static final String CHANGE_COL_OLD_NAME = "change.column.old.name";
	public static final String CHANGE_COL_NEW_NAME = "change.column.new.name";
	public static final String CHANGE_COL_NEW_TYPE = "change.column.new.type";
	public static final String CHANGE_COL_COMMENT = "change.column.comment";
	public static final String CHANGE_COL_FIRST = "change.column.first";
	public static final String CHANGE_COL_AFTER = "change.column.after";

	private final SqlIdentifier oldName;
	private final SqlTableColumn newColumn;
	private final boolean first;
	private final SqlIdentifier after;

	public SqlAlterHiveTableChangeColumn(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList partSpec, boolean cascade,
			SqlIdentifier oldName, SqlTableColumn newColumn, boolean first, SqlIdentifier after) throws ParseException {
		super(CHANGE_COLUMN, pos, tableName, partSpec, new SqlNodeList(pos), cascade);
		this.newColumn = HiveDDLUtils.deepCopyTableColumn(newColumn);
		HiveDDLUtils.convertDataTypes(newColumn);
		// set old column name
		getPropertyList().add(HiveDDLUtils.toTableOption(
				CHANGE_COL_OLD_NAME, oldName.getSimple(), oldName.getParserPosition()));
		this.oldName = oldName;
		// set new column name, type and comment
		SqlIdentifier newName = newColumn.getName();
		getPropertyList().add(HiveDDLUtils.toTableOption(
				CHANGE_COL_NEW_NAME, newName.getSimple(), newName.getParserPosition()));
		SqlDataTypeSpec newType = newColumn.getType();
		getPropertyList().add(HiveDDLUtils.toTableOption(
				CHANGE_COL_NEW_TYPE, toTypeString(newType), newType.getParserPosition()));
		if (newColumn.getComment().isPresent()) {
			SqlCharStringLiteral comment = newColumn.getComment().get();
			getPropertyList().add(HiveDDLUtils.toTableOption(CHANGE_COL_COMMENT, comment, comment.getParserPosition()));
		}
		// set whether first
		if (first) {
			getPropertyList().add(HiveDDLUtils.toTableOption(CHANGE_COL_FIRST, "true", pos));
		}
		this.first = first;
		// set after
		if (after != null) {
			getPropertyList().add(HiveDDLUtils.toTableOption(CHANGE_COL_AFTER, after.getSimple(), after.getParserPosition()));
		}
		this.after = after;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		super.unparse(writer, leftPrec, rightPrec);
		writer.keyword("CHANGE COLUMN");
		oldName.unparse(writer, leftPrec, rightPrec);
		newColumn.unparse(writer, leftPrec, rightPrec);
		if (first) {
			writer.keyword("FIRST");
		}
		if (after != null) {
			writer.keyword("AFTER");
			after.unparse(writer, leftPrec, rightPrec);
		}
		if (cascade) {
			writer.keyword("CASCADE");
		} else {
			writer.keyword("RESTRICT");
		}
	}
}

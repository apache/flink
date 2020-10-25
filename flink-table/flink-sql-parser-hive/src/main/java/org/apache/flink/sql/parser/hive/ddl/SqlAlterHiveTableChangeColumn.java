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

import org.apache.flink.sql.parser.ddl.SqlChangeColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.hive.impl.ParseException;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * ALTER DDL to change a column's name, type, position, etc.
 */
public class SqlAlterHiveTableChangeColumn extends SqlChangeColumn {

	private final SqlRegularColumn origNewColumn;
	private final boolean cascade;

	public SqlAlterHiveTableChangeColumn(SqlParserPos pos, SqlIdentifier tableName, boolean cascade,
			SqlIdentifier oldName, SqlRegularColumn newColumn, boolean first, SqlIdentifier after) throws ParseException {
		super(pos, tableName, oldName, newColumn, after, first, new SqlNodeList(pos));
		this.origNewColumn = HiveDDLUtils.deepCopyTableColumn(newColumn);
		HiveDDLUtils.convertDataTypes(newColumn);
		this.cascade = cascade;
		// set ALTER OP
		getProperties().add(HiveDDLUtils.toTableOption(
				SqlAlterHiveTable.ALTER_TABLE_OP, SqlAlterHiveTable.AlterTableOp.ALTER_COLUMNS.name(), pos));
		// set cascade
		if (cascade) {
			getProperties().add(HiveDDLUtils.toTableOption(SqlAlterHiveTable.ALTER_COL_CASCADE, "true", pos));
		}
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("ALTER TABLE");
		tableIdentifier.unparse(writer, leftPrec, rightPrec);
		SqlNodeList partitionSpec = getPartitionSpec();
		if (partitionSpec != null && partitionSpec.size() > 0) {
			writer.keyword("PARTITION");
			partitionSpec.unparse(writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
		}
		writer.keyword("CHANGE COLUMN");
		getOldName().unparse(writer, leftPrec, rightPrec);
		origNewColumn.unparse(writer, leftPrec, rightPrec);
		if (isFirst()) {
			writer.keyword("FIRST");
		}
		if (getAfter() != null) {
			writer.keyword("AFTER");
			getAfter().unparse(writer, leftPrec, rightPrec);
		}
		if (cascade) {
			writer.keyword("CASCADE");
		} else {
			writer.keyword("RESTRICT");
		}
	}
}

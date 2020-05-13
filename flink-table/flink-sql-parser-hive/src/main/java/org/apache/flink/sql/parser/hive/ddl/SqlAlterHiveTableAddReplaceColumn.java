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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.ADD_COLUMNS;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.REPLACE_COLUMNS;

/**
 * ALTER DDL to ADD or REPLACE columns for a Hive table.
 */
public class SqlAlterHiveTableAddReplaceColumn extends SqlAlterHiveTableColumn {

	public static final String ADD_REPLACE_COL_NAMES = "add.replace.column.names";
	public static final String ADD_REPLACE_COL_TYPES = "add.replace.column.types";
	public static final String ADD_REPLACE_COL_COMMENT_FORMAT = "add.replace.column.comment.%d";

	private final SqlNodeList columns;
	private final boolean replace;

	public SqlAlterHiveTableAddReplaceColumn(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList partSpec,
			boolean cascade, SqlNodeList columns, boolean replace) throws ParseException {
		super(replace ? REPLACE_COLUMNS : ADD_COLUMNS, pos, tableName, partSpec, new SqlNodeList(pos), cascade);
		this.columns = HiveDDLUtils.deepCopyColList(columns);
		HiveDDLUtils.convertDataTypes(columns);
		List<String> names = new ArrayList<>(columns.size());
		List<String> types = new ArrayList<>(columns.size());
		for (int i = 0; i < columns.size(); i++) {
			SqlTableColumn column = (SqlTableColumn) columns.get(i);
			names.add(column.getName().getSimple());
			types.add(toTypeString(column.getType()));
			if (column.getComment().isPresent()) {
				SqlCharStringLiteral comment = column.getComment().get();
				getPropertyList().add(HiveDDLUtils.toTableOption(
						String.format(ADD_REPLACE_COL_COMMENT_FORMAT, i), comment, comment.getParserPosition()));
			}
		}
		getPropertyList().add(HiveDDLUtils.toTableOption(
				ADD_REPLACE_COL_NAMES, String.join(HiveDDLUtils.COL_DELIMITER, names), columns.getParserPosition()));
		getPropertyList().add(HiveDDLUtils.toTableOption(
				ADD_REPLACE_COL_TYPES, String.join(HiveDDLUtils.COL_DELIMITER, types), columns.getParserPosition()));
		this.replace = replace;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		super.unparse(writer, leftPrec, rightPrec);
		if (replace) {
			writer.keyword("REPLACE");
		} else {
			writer.keyword("ADD");
		}
		writer.keyword("COLUMNS");
		SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
		for (SqlNode column : columns) {
			printIndent(writer);
			column.unparse(writer, leftPrec, rightPrec);
		}
		writer.newlineAndIndent();
		writer.endList(frame);
	}
}

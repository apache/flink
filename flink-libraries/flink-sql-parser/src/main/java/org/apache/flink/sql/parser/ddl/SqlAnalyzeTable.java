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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.SqlNodeList.isEmptyList;

/**
 * Analyze TABLE DDL sql call.
 * This command collects statistics about the table in addition to specified columns.
 */
public class SqlAnalyzeTable extends SqlCall {

	public static final SqlSpecialOperator OPERATOR =
			new SqlSpecialOperator("ANALYZE TABLE", SqlKind.OTHER_DDL);

	private SqlIdentifier tableName;

	private SqlNodeList columnList;

	private boolean withColumns;

	public SqlAnalyzeTable(
			SqlParserPos pos,
			SqlIdentifier tableName,
			SqlNodeList columnList,
			boolean withColumns) {
		super(pos);
		this.tableName = requireNonNull(tableName, "Table name is missing");
		this.columnList = requireNonNull(columnList, "Column list should not be null");
		this.withColumns = withColumns;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return null;
	}

	public SqlNodeList getColumnList() {
		return columnList;
	}

	public void setColumnList(SqlNodeList columnList) {
		this.columnList = columnList;
	}

	public SqlIdentifier getTableName() {
		return tableName;
	}

	public void setTableName(SqlIdentifier tableName) {
		this.tableName = tableName;
	}

	public boolean isWithColumns() {
		return withColumns;
	}

	public void unparse(
			SqlWriter writer,
			int leftPrec,
			int rightPrec) {
		writer.keyword("ANALYZE");
		writer.keyword("TABLE");
		tableName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("COMPUTE");
		writer.keyword("STATISTICS");
		if (withColumns) {
			writer.keyword("FOR");
			writer.keyword("COLUMNS");
			if (!isEmptyList(columnList)) {
				int columnMaxIndex = columnList.size() - 1;
				SqlWriter.Frame columnsFrame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
				for (int idx = 0; idx <= columnMaxIndex; idx++) {
					SqlNode column = columnList.get(idx);
					column.unparse(writer, leftPrec, rightPrec);
					if (idx != columnMaxIndex) {
						writer.sep(",", false);
					}
				}
				writer.endList(columnsFrame);
			}
		}
	}

	public void validate() {
		//todo:
	}
}

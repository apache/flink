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

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.error.SqlParseException;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * CREATE TABLE DDL sql call.
 */
public class SqlCreateTable extends SqlCreate implements ExtendedSqlNode {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

	private final SqlIdentifier tableName;

	private final SqlNodeList columnList;

	private final SqlNodeList propertyList;

	private final SqlNodeList primaryKeyList;

	private final List<SqlNodeList> uniqueKeysList;

	private final SqlNodeList partitionKeyList;

	private final SqlCharStringLiteral comment;

	public SqlCreateTable(
			SqlParserPos pos,
			SqlIdentifier tableName,
			SqlNodeList columnList,
			SqlNodeList primaryKeyList,
			List<SqlNodeList> uniqueKeysList,
			SqlNodeList propertyList,
			SqlNodeList partitionKeyList,
			SqlCharStringLiteral comment) {
		super(OPERATOR, pos, false, false);
		this.tableName = requireNonNull(tableName, "Table name is missing");
		this.columnList = requireNonNull(columnList, "Column list should not be null");
		this.primaryKeyList = primaryKeyList;
		this.uniqueKeysList = uniqueKeysList;
		this.propertyList = propertyList;
		this.partitionKeyList = partitionKeyList;
		this.comment = comment;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(tableName, columnList, primaryKeyList,
			propertyList, partitionKeyList, comment);
	}

	public SqlIdentifier getTableName() {
		return tableName;
	}

	public SqlNodeList getColumnList() {
		return columnList;
	}

	public SqlNodeList getPropertyList() {
		return propertyList;
	}

	public SqlNodeList getPartitionKeyList() {
		return partitionKeyList;
	}

	public SqlNodeList getPrimaryKeyList() {
		return primaryKeyList;
	}

	public List<SqlNodeList> getUniqueKeysList() {
		return uniqueKeysList;
	}

	public SqlCharStringLiteral getComment() {
		return comment;
	}

	public boolean isIfNotExists() {
		return ifNotExists;
	}

	public void validate() throws SqlParseException {
		Set<String> columnNames = new HashSet<>();
		if (columnList != null) {
			for (SqlNode column : columnList) {
				String columnName = null;
				if (column instanceof SqlTableColumn) {
					SqlTableColumn tableColumn = (SqlTableColumn) column;
					columnName = tableColumn.getName().getSimple();
					String typeName = tableColumn.getType().getTypeName().getSimple();
					if (SqlColumnType.getType(typeName).isUnsupported()) {
						throw new SqlParseException(
							column.getParserPosition(),
							"Not support type [" + typeName + "], at " + column.getParserPosition());
					}
				} else if (column instanceof SqlBasicCall) {
					SqlBasicCall tableColumn = (SqlBasicCall) column;
					columnName = tableColumn.getOperands()[1].toString();
				}

				if (!columnNames.add(columnName)) {
					throw new SqlParseException(
						column.getParserPosition(),
						"Duplicate column name [" + columnName + "], at " +
							column.getParserPosition());
				}
			}
		}

		if (this.primaryKeyList != null) {
			for (SqlNode primaryKeyNode : this.primaryKeyList) {
				String primaryKey = ((SqlIdentifier) primaryKeyNode).getSimple();
				if (!columnNames.contains(primaryKey)) {
					throw new SqlParseException(
						primaryKeyNode.getParserPosition(),
						"Primary key [" + primaryKey + "] not defined in columns, at " +
							primaryKeyNode.getParserPosition());
				}
			}
		}

		if (this.uniqueKeysList != null) {
			for (SqlNodeList uniqueKeys: this.uniqueKeysList) {
				for (SqlNode uniqueKeyNode : uniqueKeys) {
					String uniqueKey = ((SqlIdentifier) uniqueKeyNode).getSimple();
					if (!columnNames.contains(uniqueKey)) {
						throw new SqlParseException(
								uniqueKeyNode.getParserPosition(),
								"Unique key [" + uniqueKey + "] not defined in columns, at " + uniqueKeyNode.getParserPosition());
					}
				}
			}
		}

		if (this.partitionKeyList != null) {
			for (SqlNode partitionKeyNode : this.partitionKeyList.getList()) {
				String partitionKey = ((SqlIdentifier) partitionKeyNode).getSimple();
				if (!columnNames.contains(partitionKey)) {
					throw new SqlParseException(
						partitionKeyNode.getParserPosition(),
						"Partition column [" + partitionKey + "] not defined in columns, at "
							+ partitionKeyNode.getParserPosition());
				}
			}
		}

	}

	public boolean containsComputedColumn() {
		for (SqlNode column : columnList) {
			if (column instanceof SqlBasicCall) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns the projection format of the DDL columns(including computed columns).
	 * e.g. If we got a DDL:
	 * <pre>
	 *   create table tbl1(
	 *     col1 int,
	 *     col2 varchar,
	 *     col3 as to_timestamp(col2)
	 *   ) with (
	 *     'connector' = 'csv'
	 *   )
	 * </pre>
	 * we would return a query like:
	 *
	 * <p>"col1, col2, to_timestamp(col2) as col3", caution that the "computed column" operands
	 * have been reversed.
	 */
	public String getColumnSqlString() {
		SqlPrettyWriter writer = new SqlPrettyWriter(AnsiSqlDialect.DEFAULT);
		writer.setAlwaysUseParentheses(true);
		writer.setSelectListItemsOnSeparateLines(false);
		writer.setIndentation(0);
		writer.startList("", "");
		for (SqlNode column : columnList) {
			writer.sep(",");
			if (column instanceof SqlTableColumn) {
				SqlTableColumn tableColumn = (SqlTableColumn) column;
				tableColumn.getName().unparse(writer, 0, 0);
			} else {
				column.unparse(writer, 0, 0);
			}
		}

		return writer.toString();
	}

	@Override
	public void unparse(
		SqlWriter writer,
		int leftPrec,
		int rightPrec) {
		writer.keyword("CREATE TABLE");
		tableName.unparse(writer, leftPrec, rightPrec);
		SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
		for (SqlNode column : columnList) {
			printIndent(writer);
			if (column instanceof SqlBasicCall) {
				SqlCall call = (SqlCall) column;
				SqlCall newCall = call.getOperator().createCall(
					SqlParserPos.ZERO,
					call.operand(1),
					call.operand(0));
				newCall.unparse(writer, leftPrec, rightPrec);
			} else {
				column.unparse(writer, leftPrec, rightPrec);
			}
		}
		if (primaryKeyList != null && primaryKeyList.size() > 0) {
			printIndent(writer);
			writer.keyword("PRIMARY KEY");
			SqlWriter.Frame keyFrame = writer.startList("(", ")");
			primaryKeyList.unparse(writer, leftPrec, rightPrec);
			writer.endList(keyFrame);
		}
		if (uniqueKeysList != null && uniqueKeysList.size() > 0) {
			printIndent(writer);
			for (SqlNodeList uniqueKeyList : uniqueKeysList) {
				writer.keyword("UNIQUE");
				SqlWriter.Frame keyFrame = writer.startList("(", ")");
				uniqueKeyList.unparse(writer, leftPrec, rightPrec);
				writer.endList(keyFrame);
			}
		}

		writer.newlineAndIndent();
		writer.endList(frame);

		if (comment != null) {
			writer.newlineAndIndent();
			writer.keyword("COMMENT");
			comment.unparse(writer, leftPrec, rightPrec);
		}

		if (this.partitionKeyList != null && this.partitionKeyList.size() > 0) {
			writer.newlineAndIndent();
			writer.keyword("PARTITIONED BY");
			SqlWriter.Frame withFrame = writer.startList("(", ")");
			this.partitionKeyList.unparse(writer, leftPrec, rightPrec);
			writer.endList(withFrame);
			writer.newlineAndIndent();
		}

		if (propertyList != null) {
			writer.keyword("WITH");
			SqlWriter.Frame withFrame = writer.startList("(", ")");
			for (SqlNode property : propertyList) {
				printIndent(writer);
				property.unparse(writer, leftPrec, rightPrec);
			}
			writer.newlineAndIndent();
			writer.endList(withFrame);
		}
	}

	private void printIndent(SqlWriter writer) {
		writer.sep(",", false);
		writer.newlineAndIndent();
		writer.print("  ");
	}

	/**
	 * Table creation context.
	 */
	public static class TableCreationContext {
		public List<SqlNode> columnList = new ArrayList<>();
		public SqlNodeList primaryKeyList;
		public List<SqlNodeList> uniqueKeysList = new ArrayList<>();
	}

	public String[] fullTableName() {
		return tableName.names.toArray(new String[0]);
	}
}

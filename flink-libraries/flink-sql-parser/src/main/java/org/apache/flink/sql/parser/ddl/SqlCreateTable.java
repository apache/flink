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

import org.apache.flink.sql.parser.plan.SqlParseException;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * CREATE TABLE DDL sql call.
 */
public class SqlCreateTable extends SqlCall {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_TABLE", SqlKind.CREATE_TABLE);

	private String tableType;

	private SqlIdentifier tableName;

	private SqlNodeList columnList;

	private SqlNodeList propertyList;

	private SqlNodeList primaryKeyList;

	private List<SqlNodeList> uniqueKeysList;

	private List<IndexWrapper> indexKeysList;

	private SqlWatermark watermark;

	public SqlCreateTable(
		SqlParserPos pos,
		String tableType,
		SqlIdentifier tableName,
		SqlNodeList columnList,
		SqlNodeList primaryKeyList,
		List<SqlNodeList> uniqueKeysList,
		List<IndexWrapper> indexKeysList,
		SqlWatermark watermark,
		SqlNodeList propertyList) {
		super(pos);
		this.tableType = tableType;
		this.tableName = requireNonNull(tableName, "Table name is missing");
		this.columnList = requireNonNull(columnList, "Column list should not be null");
		this.primaryKeyList = primaryKeyList;
		this.uniqueKeysList = uniqueKeysList;
		this.indexKeysList = indexKeysList;
		this.watermark = watermark;
		this.propertyList = propertyList;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return null;
	}

	public SqlIdentifier getTableName() {
		return tableName;
	}

	public void setTableName(SqlIdentifier tableName) {
		this.tableName = tableName;
	}

	public SqlNodeList getColumnList() {
		return columnList;
	}

	public void setColumnList(SqlNodeList columnList) {
		this.columnList = columnList;
	}

	public SqlNodeList getPropertyList() {
		return propertyList;
	}

	public void setPropertyList(SqlNodeList propertyList) {
		this.propertyList = propertyList;
	}

	public SqlNodeList getPrimaryKeyList() {
		return primaryKeyList;
	}

	public void setPrimaryKeyList(SqlNodeList primaryKeyList) {
		this.primaryKeyList = primaryKeyList;
	}

	public List<SqlNodeList> getUniqueKeysList() {
		return uniqueKeysList;
	}

	public void setUniqueKeysList(List<SqlNodeList> uniqueKeysList) {
		this.uniqueKeysList = uniqueKeysList;
	}

	public List<IndexWrapper> getIndexKeysList() {
		return indexKeysList;
	}

	public void setIndexKeysList(List<IndexWrapper> indexKeysList) {
		this.indexKeysList = indexKeysList;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
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
					if (SqlColumnType.getType(typeName) == null) {
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

		if (this.indexKeysList != null) {
			for (IndexWrapper index: this.indexKeysList) {
				for (SqlNode indexKeyNode : index.indexKeys) {
					String indexKey = ((SqlIdentifier) indexKeyNode).getSimple();
					if (!columnNames.contains(indexKey)) {
						throw new SqlParseException(
								indexKeyNode.getParserPosition(),
								"IndexWrapper column [" + indexKey + "] not defined in columns, at " + indexKeyNode.getParserPosition());
					}
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
		if (indexKeysList != null && indexKeysList.size() > 0) {
			for (IndexWrapper index : indexKeysList) {
				printIndent(writer);
				if (index.unique) {
					writer.keyword("UNIQUE");
				}
				writer.keyword("INDEX");
				SqlWriter.Frame keyFrame = writer.startList("(", ")");
				index.indexKeys.unparse(writer, leftPrec, rightPrec);
				writer.endList(keyFrame);
			}
		}
		if (watermark != null) {
			printIndent(writer);
			watermark.unparse(writer, leftPrec, rightPrec);
		}
		writer.newlineAndIndent();
		writer.endList(frame);

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

	public SqlWatermark getWatermark() {
		return watermark;
	}

	/**
	 * Table temp wrapper.
	 */
	public static class TableTempWrapper {
		public List<SqlNode> columnList = new ArrayList<>();
		public SqlNodeList primaryKeyList;
		public List<SqlNodeList> uniqueKeysList = new ArrayList<>();
		public List<IndexWrapper> indexKeysList = new ArrayList<>();
		public SqlWatermark watermark;
		public String tableType;
	}

	/**
	 * IndexWrapper of Table.
	 */
	public static class IndexWrapper {
		public boolean unique;
		public SqlNodeList indexKeys;

		public IndexWrapper(boolean unique, SqlNodeList indexKeys) {
			this.unique = unique;
			this.indexKeys = indexKeys;
		}
	}
}

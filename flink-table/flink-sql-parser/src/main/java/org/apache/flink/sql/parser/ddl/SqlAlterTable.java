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
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * ALTER TABLE [[catalogName.] dataBasesName].tableName RENAME TO [[catalogName.] dataBasesName].newTableName or
 * ALTER TABLE [[catalogName.] dataBasesName].tableName SET ( name=value [, name=value]*) sql call.
 */
public class SqlAlterTable extends SqlCall {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE);

	private final SqlIdentifier tableIdentifier;
	private final SqlIdentifier newTableIdentifier;

	private final SqlNodeList propertyList;
	private final boolean isRename;

	public SqlAlterTable(
			SqlParserPos pos,
			SqlIdentifier tableName,
			SqlIdentifier newTableName,
			SqlNodeList propertyList,
			boolean isRename) {
		super(pos);
		this.tableIdentifier = requireNonNull(tableName, "tableName should not be null");
		this.newTableIdentifier = newTableName;
		this.propertyList = requireNonNull(propertyList, "propertyList should not be null");
		this.isRename = isRename;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(tableIdentifier, newTableIdentifier, propertyList);
	}

	public SqlIdentifier getTableName() {
		return tableIdentifier;
	}

	public boolean isRename() {
		return isRename;
	}

	public SqlNodeList getPropertyList() {
		return propertyList;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("ALTER TABLE");
		tableIdentifier.unparse(writer, leftPrec, rightPrec);
		if (isRename) {
			writer.keyword("RENAME TO");
			newTableIdentifier.unparse(writer, leftPrec, rightPrec);
		} else {
			writer.keyword("SET");
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

	public String[] fullTableName() {
		return tableIdentifier.names.toArray(new String[0]);
	}

	public String[] fullNewTableName() {
		return newTableIdentifier.names.toArray(new String[0]);
	}
}

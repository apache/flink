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

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * DROP TABLE DDL sql call.
 */
public class SqlDropTable extends SqlDrop implements ExtendedSqlNode {
	private static final SqlOperator OPERATOR =
		new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE);

	private SqlIdentifier tableName;
	private boolean ifExists;

	public SqlDropTable(SqlParserPos pos, SqlIdentifier tableName, boolean ifExists) {
		super(OPERATOR, pos, ifExists);
		this.tableName = tableName;
		this.ifExists = ifExists;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(tableName);
	}

	public SqlIdentifier getTableName() {
		return tableName;
	}

	public void setTableName(SqlIdentifier viewName) {
		this.tableName = viewName;
	}

	public boolean getIfExists() {
		return this.ifExists;
	}

	public void setIfExists(boolean ifExists) {
		this.ifExists = ifExists;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("DROP");
		writer.keyword("TABLE");
		if (ifExists) {
			writer.keyword("IF EXISTS");
		}
		tableName.unparse(writer, leftPrec, rightPrec);
	}

	public void validate() {
		// no-op
	}

	public String[] fullTableName() {
		return tableName.names.toArray(new String[0]);
	}
}

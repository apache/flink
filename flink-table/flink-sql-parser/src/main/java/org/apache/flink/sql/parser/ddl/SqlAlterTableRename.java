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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * ALTER TABLE [[catalogName.] dataBasesName].tableName RENAME TO [[catalogName.] dataBasesName].newTableName.
 */
public class SqlAlterTableRename extends SqlAlterTable {

	private final SqlIdentifier newTableIdentifier;

	public SqlAlterTableRename(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier newTableName) {
		super(pos, tableName);
		this.newTableIdentifier = requireNonNull(newTableName, "new tableName should not be null");
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(tableIdentifier, newTableIdentifier);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		super.unparse(writer, leftPrec, rightPrec);
		writer.keyword("RENAME TO");
		newTableIdentifier.unparse(writer, leftPrec, rightPrec);
	}

	public String[] fullNewTableName() {
		return newTableIdentifier.names.toArray(new String[0]);
	}
}

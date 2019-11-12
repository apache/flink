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
 * DROP VIEW DDL sql call.
 */
public class SqlDropView extends SqlDrop implements ExtendedSqlNode {
	private static final SqlOperator OPERATOR =
		new SqlSpecialOperator("DROP VIEW", SqlKind.DROP_VIEW);

	private final SqlIdentifier viewName;

	public SqlDropView(SqlParserPos pos, SqlIdentifier viewName, boolean ifExists) {
		super(OPERATOR, pos, ifExists);
		this.viewName = viewName;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(viewName);
	}

	public SqlIdentifier getViewName() {
		return viewName;
	}

	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("DROP");
		writer.keyword("VIEW");
		if (ifExists) {
			writer.keyword("IF EXISTS");
		}
		viewName.unparse(writer, leftPrec, rightPrec);
	}

	public void validate() {
		// no-op
	}

	public String[] fullViewName() {
		return viewName.names.toArray(new String[0]);
	}
}

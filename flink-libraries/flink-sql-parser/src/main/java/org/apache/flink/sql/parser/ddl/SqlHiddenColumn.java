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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Sql hidden column sql call, a hidden column is excluded in select star.
 */
public class SqlHiddenColumn extends SqlCall {

	private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("HIDDEN_COLUMN", SqlKind.OTHER);

	private SqlIdentifier name;
	private String type;
	private boolean hidden;

	public SqlHiddenColumn(SqlParserPos pos, SqlNode name, String type, boolean hidden) {
		super(pos);
		this.name = (SqlIdentifier) requireNonNull(name);
		this.type = requireNonNull(type);
		this.hidden = hidden;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(name);
	}

	public SqlIdentifier getName() {
		return name;
	}

	public void setName(SqlIdentifier name) {
		this.name = name;
	}

	public boolean isHidden() {
		return hidden;
	}

	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		name.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AS");
		writer.print(type + "()");
		if (hidden) {
			writer.keyword("HIDDEN");
		}
	}

	@Override
	public void validate(SqlValidator validator, SqlValidatorScope scope) {

	}
}

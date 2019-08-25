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

import org.apache.flink.sql.parser.type.ExtendedSqlType;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Table column of a CREATE TABLE DDL.
 */
public class SqlTableColumn extends SqlCall {
	private static final SqlSpecialOperator OPERATOR =
		new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

	private SqlIdentifier name;
	private SqlDataTypeSpec type;
	private SqlCharStringLiteral comment;

	public SqlTableColumn(SqlIdentifier name,
			SqlDataTypeSpec type,
			SqlCharStringLiteral comment,
			SqlParserPos pos) {
		super(pos);
		this.name = requireNonNull(name, "Column name should not be null");
		this.type = requireNonNull(type, "Column type should not be null");
		this.comment = comment;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(name, type, comment);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		this.name.unparse(writer, leftPrec, rightPrec);
		writer.print(" ");
		ExtendedSqlType.unparseType(type, writer, leftPrec, rightPrec);
		if (this.comment != null) {
			writer.print(" COMMENT ");
			this.comment.unparse(writer, leftPrec, rightPrec);
		}
	}

	public SqlIdentifier getName() {
		return name;
	}

	public void setName(SqlIdentifier name) {
		this.name = name;
	}

	public SqlDataTypeSpec getType() {
		return type;
	}

	public void setType(SqlDataTypeSpec type) {
		this.type = type;
	}

	public SqlCharStringLiteral getComment() {
		return comment;
	}

	public void setComment(SqlCharStringLiteral comment) {
		this.comment = comment;
	}
}

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
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Table computed column of a CREATE TABLE DDL.
 */
public class SqlTableComputedColumn extends SqlCall {
	private static final SqlSpecialOperator OPERATOR =
		new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

	private SqlNode identifier;
	private SqlNode expr;
	private SqlCharStringLiteral comment;

	public SqlTableComputedColumn(SqlNode identifier, SqlNode expr, @Nullable SqlCharStringLiteral comment, SqlParserPos pos) {
		super(pos);
		this.identifier = requireNonNull(identifier, "Column identifier should not be null");
		this.expr = requireNonNull(expr, "Column expression should not be null");
		this.comment = comment;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(identifier, expr, comment);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		this.identifier.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AS");
		this.expr.unparse(writer, leftPrec, rightPrec);
		if (this.comment != null) {
			writer.print(" COMMENT ");
			this.comment.unparse(writer, leftPrec, rightPrec);
		}
	}

	public SqlNode getIdentifier() {
		return identifier;
	}

	public void setIdentifier(SqlNode identifier) {
		this.identifier = identifier;
	}

	public SqlNode getExpr() {
		return expr;
	}

	public void setExpr(SqlNode expr) {
		this.expr = expr;
	}

	public Optional<SqlCharStringLiteral> getComment() {
		return Optional.ofNullable(comment);
	}

	public void setComment(SqlCharStringLiteral comment) {
		this.comment = comment;
	}
}

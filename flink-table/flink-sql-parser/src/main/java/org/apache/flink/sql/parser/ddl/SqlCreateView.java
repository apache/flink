/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * CREATE VIEW DDL sql call.
 */
public class SqlCreateView extends SqlCreate implements ExtendedSqlNode {
	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_VIEW", SqlKind.CREATE_VIEW);

	private final SqlIdentifier viewName;
	private final SqlNodeList fieldList;
	private final SqlNode query;

	@Nullable
	private final SqlCharStringLiteral comment;

	public SqlCreateView(
			SqlParserPos pos,
			SqlIdentifier viewName,
			SqlNodeList fieldList,
			SqlNode query,
			boolean replace,
			SqlCharStringLiteral comment) {
		super(OPERATOR, pos, replace, false);
		this.viewName = requireNonNull(viewName, "viewName should not be null");
		this.fieldList = requireNonNull(fieldList, "fieldList should not be null");
		this.query = requireNonNull(query, "query should not be null");
		this.comment = comment;
	}

	@Override
	public List<SqlNode> getOperandList() {
		List<SqlNode> ops = new ArrayList<>();
		ops.add(viewName);
		ops.add(fieldList);
		ops.add(query);
		ops.add(SqlLiteral.createBoolean(getReplace(), SqlParserPos.ZERO));
		return ops;
	}

	public SqlIdentifier getViewName() {
		return viewName;
	}

	public SqlNodeList getFieldList() {
		return fieldList;
	}

	public SqlNode getQuery() {
		return query;
	}

	public Optional<SqlCharStringLiteral> getComment() {
		return Optional.ofNullable(comment);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("CREATE");
		if (getReplace()) {
			writer.keyword("OR REPLACE");
		}
		writer.keyword("VIEW");
		viewName.unparse(writer, leftPrec, rightPrec);
		if (fieldList.size() > 0) {
			fieldList.unparse(writer, 1, rightPrec);
		}
		if (comment != null) {
			writer.newlineAndIndent();
			writer.keyword("COMMENT");
			comment.unparse(writer, leftPrec, rightPrec);
		}
		writer.newlineAndIndent();
		writer.keyword("AS");
		writer.newlineAndIndent();
		query.unparse(writer, leftPrec, rightPrec);
	}

	private void printIndent(SqlWriter writer) {
		writer.sep(",", false);
		writer.newlineAndIndent();
		writer.print("  ");
	}

	@Override
	public void validate() throws SqlValidateException {
		// no-op
	}
}

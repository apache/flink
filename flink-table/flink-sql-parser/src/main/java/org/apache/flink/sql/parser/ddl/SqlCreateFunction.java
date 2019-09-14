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
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * CREATE FUNCTION DDL sql call.
 */
public class SqlCreateFunction extends SqlCreate implements ExtendedSqlNode {
	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

	private SqlIdentifier functionName;
	private SqlCharStringLiteral functionClassName;
	private SqlCharStringLiteral functionLanguage;
	private boolean isSystemFunction;

	public SqlCreateFunction(
		SqlParserPos pos,
		SqlIdentifier functionName,
		SqlCharStringLiteral functionClassName,
		SqlCharStringLiteral functionLanguage,
		boolean ifNotExists,
		boolean isSystemFunction) {
		super(OPERATOR, pos, false, ifNotExists);
		this.functionName = requireNonNull(functionName);
		this.functionClassName = requireNonNull(functionClassName);
		this.isSystemFunction = requireNonNull(isSystemFunction);
		this.functionLanguage = functionLanguage;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Nonnull
	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(functionName, functionClassName, functionLanguage);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("CREATE");
		if (isSystemFunction) {
			writer.keyword("TEMPORARY SYSTEM");
		} else {
			writer.keyword("TEMPORARY");
		}
		writer.keyword("FUNCTION");
		if (ifNotExists) {
			writer.keyword("IF NOT EXISTS");
		}
		functionName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AS");
		functionClassName.unparse(writer, leftPrec, rightPrec);
		if (functionLanguage != null) {
			writer.keyword("LANGUAGE");
			functionLanguage.unparse(writer, leftPrec, rightPrec);
		}
	}

	@Override
	public void validate() throws SqlValidateException {
		// no-op
	}

	public boolean isIfNotExists() {
		return ifNotExists;
	}

	public boolean isSystemFunction() {
		return isSystemFunction;
	}

	public SqlIdentifier getFunctionName() {
		return this.functionName;
	}

	public SqlCharStringLiteral getFunctionClassName() {
		return this.functionClassName;
	}

	public SqlCharStringLiteral getFunctionLanguage() {
		return this.functionLanguage;
	}

	public String[] fullFunctionName() {
		return functionName.names.toArray(new String[0]);
	}
}

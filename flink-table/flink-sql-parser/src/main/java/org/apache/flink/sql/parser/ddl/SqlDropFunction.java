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

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * DROP FUNCTION DDL sql call.
 */
public class SqlDropFunction extends SqlDrop implements ExtendedSqlNode {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP FUNCTION", SqlKind.DROP_FUNCTION);

	private final SqlIdentifier functionName;

	private final String functionLanguage;

	private final boolean hasTemporary;

	private final boolean isSystemFunction;

	public SqlDropFunction(
		SqlParserPos pos,
		SqlIdentifier functionName,
		String functionLanguage,
		boolean ifExists,
		boolean hasTemporary,
		boolean isSystemFunction) {
		super(OPERATOR, pos, ifExists);
		this.functionName = requireNonNull(functionName);
		this.isSystemFunction = requireNonNull(isSystemFunction);
		this.hasTemporary = hasTemporary;
		this.functionLanguage = functionLanguage;
	}

	@Nonnull
	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(functionName);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("DROP");
		if (hasTemporary) {
			writer.keyword("TEMPORARY");
		}
		if (isSystemFunction) {
			writer.keyword("SYSTEM");
		}
		writer.keyword("FUNCTION");
		if (ifExists) {
			writer.keyword("IF EXISTS");
		}
		functionName.unparse(writer, leftPrec, rightPrec);
		if (functionLanguage != null) {
			writer.keyword("LANGUAGE");
			writer.keyword(functionLanguage);
		}
	}

	@Override
	public void validate() throws SqlValidateException {
		// no-op
	}

	public String getFunctionLanguage() {
		return this.functionLanguage;
	}

	public String[] fullFunctionName() {
		return functionName.names.toArray(new String[0]);
	}

	public boolean getIfExists() {
		return this.ifExists;
	}
}

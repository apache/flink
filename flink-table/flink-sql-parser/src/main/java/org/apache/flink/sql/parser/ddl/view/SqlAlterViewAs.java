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

package org.apache.flink.sql.parser.ddl.view;

import org.apache.flink.sql.parser.SqlUnparseUtils;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

/** ALTER DDL to change a view's query. */
public class SqlAlterViewAs extends SqlAlterView {

    private static final SqlSpecialOperator AS_OPERATOR =
            new SqlSpecialOperator("ALTER VIEW AS", SqlKind.ALTER_VIEW) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlAlterViewAs(
                            pos, (SqlIdentifier) operands[0], operands[1], SqlParserPos.ZERO);
                }
            };

    private final SqlNode newQuery;

    private final SqlParserPos asQueryKeywordPos;

    public SqlAlterViewAs(
            SqlParserPos pos,
            SqlIdentifier viewIdentifier,
            SqlNode newQuery,
            SqlParserPos asQueryKeywordPos) {
        super(pos, viewIdentifier);
        this.newQuery = newQuery;
        this.asQueryKeywordPos = asQueryKeywordPos;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return AS_OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, newQuery);
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseAsQuery(newQuery, writer, leftPrec, rightPrec);
    }

    public SqlNode getNewQuery() {
        return newQuery;
    }

    /** Returns the parser position of the {@code AS} keyword. */
    public SqlParserPos getAsQueryKeywordPos() {
        return asQueryKeywordPos;
    }
}

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

package org.apache.flink.sql.parser.expr;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * The infix cast operator {@code expr::type}.
 *
 * <p>The parser uses this operator only to group {@code ::} by precedence. {@link #createCall}
 * returns a standard {@code CAST} call, so {@code a::INT} parses to the same tree as {@code CAST(a
 * AS INT)} and no later stage sees this operator.
 */
public class SqlInfixCastOperator extends SqlBinaryOperator {

    public static final SqlInfixCastOperator INSTANCE = new SqlInfixCastOperator();

    private SqlInfixCastOperator() {
        // Calcite's INFIX_CAST precedence: tighter than arithmetic and unary minus, looser than
        // item access and field access. Left-associative, so a::INT::STRING casts twice in order.
        super("::", SqlKind.CAST, 94, true, null, null, null);
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return SqlStdOperatorTable.CAST.createCall(functionQualifier, pos, operands);
    }
}

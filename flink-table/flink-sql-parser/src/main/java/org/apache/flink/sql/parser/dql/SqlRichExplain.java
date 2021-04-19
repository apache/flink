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

package org.apache.flink.sql.parser.dql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

/** EXPLAIN (PLAN FOR)* STATEMENT sql call. */
public class SqlRichExplain extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("EXPLAIN", SqlKind.EXPLAIN);

    private SqlNode statement;

    public SqlRichExplain(SqlParserPos pos, SqlNode statement) {
        super(pos);
        this.statement = statement;
    }

    public SqlNode getStatement() {
        return statement;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(statement);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EXPLAIN");
        statement.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        if (i == 0) {
            statement = operand;
        } else {
            throw new UnsupportedOperationException(
                    "SqlRichExplain SqlNode only support index equals 1");
        }
    }
}

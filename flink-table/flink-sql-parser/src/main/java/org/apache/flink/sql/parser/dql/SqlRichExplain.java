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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * EXPLAIN [PLAN FOR | (ESTIMATED_COST | CHANGELOG_MODE | JSON_EXECUTION_PLAN |
 * ANALYZED_PHYSICAL_PLAN) (,(ESTIMATED_COST | CHANGELOG_MODE | JSON_EXECUTION_PLAN |
 * PLAN_ADVICE))*] STATEMENT sql call.
 */
public class SqlRichExplain extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("EXPLAIN", SqlKind.EXPLAIN);

    private SqlNode statement;
    private final Set<String> explainDetails;

    public SqlRichExplain(SqlParserPos pos, SqlNode statement) {
        this(pos, statement, new HashSet<>());
    }

    public SqlRichExplain(SqlParserPos pos, SqlNode statement, Set<String> explainDetails) {
        super(pos);
        this.statement = statement;
        this.explainDetails = explainDetails;
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

    public Set<String> getExplainDetails() {
        return explainDetails;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EXPLAIN");
        if (!explainDetails.isEmpty()) {
            writer.keyword(String.join(", ", explainDetails));
        }
        statement.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        if (i == 0) {
            statement = operand;
        } else {
            throw new UnsupportedOperationException(
                    "SqlRichExplain SqlNode only support index equals 0");
        }
    }
}

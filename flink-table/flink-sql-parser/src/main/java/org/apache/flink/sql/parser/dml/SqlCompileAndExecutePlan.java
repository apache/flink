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

package org.apache.flink.sql.parser.dml;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.List;

/**
 * AST node for {@code COMPILE AND EXECUTE PLAN 'planfile' FOR [DML]}. DML can be either a {@link
 * RichSqlInsert} or a {@link SqlStatementSet}.
 */
@Internal
public class SqlCompileAndExecutePlan extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COMPILE AND EXECUTE PLAN", SqlKind.OTHER);

    private final SqlNode planFile;
    private SqlNode operand;

    public SqlCompileAndExecutePlan(SqlParserPos pos, SqlNode planFile, SqlNode operand) {

        super(pos);
        this.planFile = planFile;
        this.operand = checkOperand(operand);
    }

    public String getPlanFile() {
        return ((NlsString) SqlLiteral.value(planFile)).getValue();
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(operand);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("COMPILE");
        writer.keyword("AND");
        writer.keyword("EXECUTE");
        writer.keyword("PLAN");
        planFile.unparse(writer, leftPrec, rightPrec);
        writer.keyword("FOR");
        operand.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        if (i == 0) {
            this.operand = checkOperand(operand);
        } else {
            throw new UnsupportedOperationException(
                    "SqlCompileAndExecutePlan supports only one operand with index 0");
        }
    }

    private SqlNode checkOperand(SqlNode operand) {
        if (!(operand instanceof RichSqlInsert || operand instanceof SqlStatementSet)) {
            throw new UnsupportedOperationException(
                    "SqlCompileAndExecutePlan supports only RichSqlInsert or SqlStatementSet as operand");
        }
        return operand;
    }
}

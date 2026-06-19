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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;

/**
 * Statement Set contains a group of inserts. eg:
 *
 * <ul>
 *   execute statement set begin insert into A select * from B; insert into C select * from D; end
 * </ul>
 */
public class SqlStatementSet extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("Statement Set", SqlKind.OTHER);

    private final ArrayList<RichSqlInsert> inserts = new ArrayList<>();

    public SqlStatementSet(List<RichSqlInsert> inserts, SqlParserPos pos) {
        super(pos);
        this.inserts.addAll(inserts);
    }

    public List<RichSqlInsert> getInserts() {
        return inserts;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return new ArrayList<>(inserts);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("STATEMENT SET BEGIN");
        writer.newlineAndIndent();
        inserts.forEach(
                insert -> {
                    insert.unparse(
                            writer,
                            insert.getOperator().getLeftPrec(),
                            insert.getOperator().getRightPrec());
                    writer.sep(";");
                    writer.newlineAndIndent();
                });
        writer.keyword("END");
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        if (!(operand instanceof RichSqlInsert)) {
            throw new UnsupportedOperationException(
                    "SqlStatementSet SqlNode only support RichSqlInsert as operand");
        }
        inserts.set(i, (RichSqlInsert) operand);
    }
}

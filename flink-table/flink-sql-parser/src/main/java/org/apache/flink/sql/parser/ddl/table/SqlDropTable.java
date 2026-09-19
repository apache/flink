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

package org.apache.flink.sql.parser.ddl.table;

import org.apache.flink.sql.parser.ddl.SqlDropObject;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/** DROP TABLE DDL sql call. */
public class SqlDropTable extends SqlDropObject {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlDropTable(
                            pos,
                            (SqlIdentifier) operands[0],
                            ((SqlLiteral) operands[1]).booleanValue(),
                            ((SqlLiteral) operands[2]).booleanValue());
                }
            };

    private final boolean isTemporary;

    public SqlDropTable(
            SqlParserPos pos, SqlIdentifier tableName, boolean ifExists, boolean isTemporary) {
        super(OPERATOR, pos, tableName, ifExists);
        this.isTemporary = isTemporary;
    }

    public boolean isTemporary() {
        return this.isTemporary;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(
                name,
                SqlLiteral.createBoolean(ifExists, SqlParserPos.ZERO),
                SqlLiteral.createBoolean(isTemporary, SqlParserPos.ZERO));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        if (isTemporary) {
            writer.keyword("TEMPORARY");
        }
        writer.keyword("TABLE");
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
    }
}

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

/** ALTER DDL to rename a view. */
public class SqlAlterViewRename extends SqlAlterView {

    private static final SqlSpecialOperator RENAME_OPERATOR =
            new SqlSpecialOperator("ALTER VIEW RENAME", SqlKind.ALTER_VIEW) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlAlterViewRename(
                            pos, (SqlIdentifier) operands[0], (SqlIdentifier) operands[1]);
                }
            };

    private final SqlIdentifier newViewIdentifier;

    public SqlAlterViewRename(
            SqlParserPos pos, SqlIdentifier viewIdentifier, SqlIdentifier newViewIdentifier) {
        super(pos, viewIdentifier);
        this.newViewIdentifier = newViewIdentifier;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return RENAME_OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, newViewIdentifier);
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        writer.keyword("RENAME TO");
        newViewIdentifier.unparse(writer, leftPrec, rightPrec);
    }

    public String[] fullNewViewName() {
        return newViewIdentifier.names.toArray(new String[0]);
    }
}

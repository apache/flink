/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.sql.parser.ddl.position;

import org.apache.flink.sql.parser.ddl.SqlTableColumn;

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
import javax.annotation.Nullable;

import java.util.List;

/** SqlNode to describe table column and its position. */
public class SqlTableColumnPosition extends SqlCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("SqlTableColumnPosition", SqlKind.OTHER);

    private final SqlTableColumn column;
    @Nullable private final SqlLiteral positionSpec;
    @Nullable private final SqlIdentifier referencedColumn;

    public SqlTableColumnPosition(
            SqlParserPos pos,
            SqlTableColumn column,
            @Nullable SqlLiteral positionSpec,
            @Nullable SqlIdentifier referencedColumn) {
        super(pos);
        this.column = column;
        this.positionSpec = positionSpec;
        this.referencedColumn = referencedColumn;
    }

    public boolean isFirstColumn() {
        return positionSpec != null
                && positionSpec.getValueAs(SqlColumnPosSpec.class) == SqlColumnPosSpec.FIRST;
    }

    public boolean isAfterReferencedColumn() {
        return positionSpec != null
                && positionSpec.getValueAs(SqlColumnPosSpec.class) == SqlColumnPosSpec.AFTER
                && referencedColumn != null;
    }

    public SqlTableColumn getColumn() {
        return column;
    }

    public SqlLiteral getPositionSpec() {
        return positionSpec;
    }

    @Nullable
    public SqlIdentifier getAfterReferencedColumn() {
        return referencedColumn;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(column, positionSpec, referencedColumn);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        column.unparse(writer, leftPrec, rightPrec);
        if (isFirstColumn()) {
            positionSpec.unparse(writer, leftPrec, rightPrec);
        } else if (isAfterReferencedColumn()) {
            positionSpec.unparse(writer, leftPrec, rightPrec);
            referencedColumn.unparse(writer, leftPrec, rightPrec);
        } else {
            // default no refer other column
        }
    }
}

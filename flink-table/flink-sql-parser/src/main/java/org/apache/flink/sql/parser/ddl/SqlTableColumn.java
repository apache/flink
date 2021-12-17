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

import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** Table column of a CREATE TABLE DDL. */
public abstract class SqlTableColumn extends SqlCall {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

    protected final SqlIdentifier name;

    protected final SqlNode comment;

    private SqlTableColumn(SqlParserPos pos, SqlIdentifier name, @Nullable SqlNode comment) {
        super(pos);
        this.name = requireNonNull(name, "Column name should not be null");
        this.comment = comment;
    }

    protected abstract void unparseColumn(SqlWriter writer, int leftPrec, int rightPrec);

    @Override
    public @Nonnull SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);

        unparseColumn(writer, leftPrec, rightPrec);

        if (comment != null) {
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlIdentifier getName() {
        return name;
    }

    public Optional<SqlNode> getComment() {
        return Optional.ofNullable(comment);
    }

    /** A regular, physical column. */
    public static class SqlRegularColumn extends SqlTableColumn {

        private SqlDataTypeSpec type;

        private final @Nullable SqlTableConstraint constraint;

        public SqlRegularColumn(
                SqlParserPos pos,
                SqlIdentifier name,
                @Nullable SqlNode comment,
                SqlDataTypeSpec type,
                @Nullable SqlTableConstraint constraint) {
            super(pos, name, comment);
            this.type = requireNonNull(type, "Column type should not be null");
            this.constraint = constraint;
        }

        public SqlDataTypeSpec getType() {
            return type;
        }

        public void setType(SqlDataTypeSpec type) {
            this.type = type;
        }

        public Optional<SqlTableConstraint> getConstraint() {
            return Optional.ofNullable(constraint);
        }

        @Override
        protected void unparseColumn(SqlWriter writer, int leftPrec, int rightPrec) {
            type.unparse(writer, leftPrec, rightPrec);
            if (this.type.getNullable() != null && !this.type.getNullable()) {
                // Default is nullable.
                writer.keyword("NOT NULL");
            }
            if (constraint != null) {
                constraint.unparse(writer, leftPrec, rightPrec);
            }
        }

        @Override
        public @Nonnull List<SqlNode> getOperandList() {
            return ImmutableNullableList.of(name, type, constraint, comment);
        }
    }

    /** A column derived from metadata. */
    public static class SqlMetadataColumn extends SqlTableColumn {

        private final SqlDataTypeSpec type;

        private final @Nullable SqlNode metadataAlias;

        private final boolean isVirtual;

        public SqlMetadataColumn(
                SqlParserPos pos,
                SqlIdentifier name,
                @Nullable SqlNode comment,
                SqlDataTypeSpec type,
                @Nullable SqlNode metadataAlias,
                boolean isVirtual) {
            super(pos, name, comment);
            this.type = requireNonNull(type, "Column type should not be null");
            this.metadataAlias = metadataAlias;
            this.isVirtual = isVirtual;
        }

        public SqlDataTypeSpec getType() {
            return type;
        }

        public Optional<String> getMetadataAlias() {
            return Optional.ofNullable(metadataAlias)
                    .map(alias -> ((NlsString) SqlLiteral.value(alias)).getValue());
        }

        public boolean isVirtual() {
            return isVirtual;
        }

        @Override
        protected void unparseColumn(SqlWriter writer, int leftPrec, int rightPrec) {
            type.unparse(writer, leftPrec, rightPrec);
            if (this.type.getNullable() != null && !this.type.getNullable()) {
                // Default is nullable.
                writer.keyword("NOT NULL");
            }
            writer.keyword("METADATA");
            if (metadataAlias != null) {
                writer.keyword("FROM");
                metadataAlias.unparse(writer, leftPrec, rightPrec);
            }
            if (isVirtual) {
                writer.keyword("VIRTUAL");
            }
        }

        @Override
        public @Nonnull List<SqlNode> getOperandList() {
            return ImmutableNullableList.of(name, type, comment);
        }
    }

    /** A column derived from an expression. */
    public static class SqlComputedColumn extends SqlTableColumn {

        private final SqlNode expr;

        public SqlComputedColumn(
                SqlParserPos pos, SqlIdentifier name, @Nullable SqlNode comment, SqlNode expr) {
            super(pos, name, comment);
            this.expr = requireNonNull(expr, "Column expression should not be null");
        }

        public SqlNode getExpr() {
            return expr;
        }

        @Override
        protected void unparseColumn(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword("AS");
            expr.unparse(writer, leftPrec, rightPrec);
        }

        @Override
        public @Nonnull List<SqlNode> getOperandList() {
            return ImmutableNullableList.of(name, expr, comment);
        }
    }
}

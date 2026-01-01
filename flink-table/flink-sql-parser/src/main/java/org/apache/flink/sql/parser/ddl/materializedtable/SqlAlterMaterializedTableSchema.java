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

package org.apache.flink.sql.parser.ddl.materializedtable;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlConstraintValidator;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Abstract class to describe statements which are used to alter schema for materialized tables. See
 * examples in javadoc for child classes.
 */
public abstract class SqlAlterMaterializedTableSchema extends SqlAlterMaterializedTable
        implements ExtendedSqlNode {

    protected final SqlNodeList columnList;
    protected final List<SqlTableConstraint> constraints;
    protected final @Nullable SqlWatermark watermark;

    public SqlAlterMaterializedTableSchema(
            SqlParserPos pos,
            SqlIdentifier materializedTableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> constraints,
            @Nullable SqlWatermark sqlWatermark) {
        super(pos, materializedTableName);
        this.columnList = columnList;
        this.constraints = constraints;
        this.watermark = sqlWatermark;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                name, columnList, new SqlNodeList(constraints, SqlParserPos.ZERO), watermark);
    }

    @Override
    public void validate() throws SqlValidateException {
        SqlConstraintValidator.validateAndChangeColumnNullability(constraints, getColumns());
    }

    public SqlNodeList getColumnPositions() {
        return columnList;
    }

    public Optional<SqlWatermark> getWatermark() {
        return Optional.ofNullable(watermark);
    }

    public List<SqlTableConstraint> getConstraints() {
        return constraints;
    }

    public Optional<SqlTableConstraint> getFullConstraint() {
        List<SqlTableConstraint> primaryKeys =
                SqlConstraintValidator.getFullConstraints(constraints, getColumns());
        return primaryKeys.isEmpty() ? Optional.empty() : Optional.of(primaryKeys.get(0));
    }

    private SqlNodeList getColumns() {
        return new SqlNodeList(
                columnList.getList().stream()
                        .map(columnPos -> ((SqlTableColumnPosition) columnPos).getColumn())
                        .collect(Collectors.toList()),
                SqlParserPos.ZERO);
    }

    protected abstract String getAlterOperation();

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        writer.keyword(getAlterOperation());
        SqlUnparseUtils.unparseTableSchema(
                columnList, constraints, watermark, writer, leftPrec, rightPrec);
    }

    /**
     * Example: DDL like the below for adding column(s)/constraint/watermark.
     *
     * <p>Note: adding or modifying persisted columns is not supported, only computed or metadata
     *
     * <pre>{@code
     * -- add single column
     * ALTER MATERIALIZED TABLE myMaterializedTable ADD c1 AS current_timestamp COMMENT 'new_column docs';
     *
     * -- add multiple columns, constraint, and watermark
     * ALTER MATERIALIZED TABLE myMaterializedTable ADD (
     *     ts AS current_timestamp FIRST,
     *     col_meta INT METADATA FROM 'mk1' VIRTUAL AFTER col_b,
     *     PRIMARY KEY (id) NOT ENFORCED,
     *     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
     * );
     *
     * }</pre>
     */
    public static class SqlAlterMaterializedTableAddSchema extends SqlAlterMaterializedTableSchema {
        public SqlAlterMaterializedTableAddSchema(
                SqlParserPos pos,
                SqlIdentifier materializedTableName,
                SqlNodeList columnList,
                List<SqlTableConstraint> constraints,
                @Nullable SqlWatermark sqlWatermark) {
            super(pos, materializedTableName, columnList, constraints, sqlWatermark);
        }

        @Override
        protected String getAlterOperation() {
            return "ADD";
        }
    }

    /**
     * Example: DDL like the below for modifying column(s)/constraint/watermark.
     *
     * <p>Note: adding or modifying persisted columns is not supported, only computed or metadata
     *
     * <pre>{@code
     * -- modify single column
     * ALTER MATERIALIZED TABLE myMaterializedTable MODIFY c1 AS current_timestamp COMMENT 'new_column docs';
     *
     * -- modify multiple columns, constraint, and watermark
     * ALTER MATERIALIZED TABLE myMaterializedTable MODIFY (
     *     ts AS current_timestamp FIRST,
     *     col_meta INT METADATA FROM 'mk1' VIRTUAL AFTER col_b,
     *     PRIMARY KEY (id) NOT ENFORCED,
     *     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
     * );
     *
     * }</pre>
     */
    public static class SqlAlterMaterializedTableModifySchema
            extends SqlAlterMaterializedTableSchema {
        public SqlAlterMaterializedTableModifySchema(
                SqlParserPos pos,
                SqlIdentifier materializedTableName,
                SqlNodeList columnList,
                List<SqlTableConstraint> constraints,
                @Nullable SqlWatermark sqlWatermark) {
            super(pos, materializedTableName, columnList, constraints, sqlWatermark);
        }

        @Override
        protected String getAlterOperation() {
            return "MODIFY";
        }
    }

    /**
     * Abstract class to describe statements which are used to drop schema components while altering
     * schema of materialized tables. See examples in javadoc for child classes.
     */
    public abstract static class SqlAlterMaterializedTableDropSchema
            extends SqlAlterMaterializedTableSchema {
        public SqlAlterMaterializedTableDropSchema(
                SqlParserPos pos, SqlIdentifier materializedTableName) {
            super(pos, materializedTableName, SqlNodeList.EMPTY, List.of(), null);
        }

        protected abstract void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec);

        @Override
        protected String getAlterOperation() {
            return "DROP";
        }

        @Override
        public final void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
            super.unparseAlterOperation(writer, leftPrec, rightPrec);
            unparseDropOperation(writer, leftPrec, rightPrec);
        }
    }

    /** ALTER MATERIALIZED TABLE [catalog_name.][db_name.]materialized_table_name DROP WATERMARK. */
    public static class SqlAlterMaterializedTableDropWatermark
            extends SqlAlterMaterializedTableDropSchema {
        public SqlAlterMaterializedTableDropWatermark(
                SqlParserPos pos, SqlIdentifier materializedTableName) {
            super(pos, materializedTableName);
        }

        @Override
        public List<SqlNode> getOperandList() {
            return List.of(name);
        }

        @Override
        public void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword("WATERMARK");
        }
    }

    /**
     * ALTER MATERIALIZED TABLE [catalog_name.][db_name.]materialized_table_name DROP PRIMARY KEY.
     */
    public static class SqlAlterMaterializedTableDropPrimaryKey
            extends SqlAlterMaterializedTableDropSchema {
        public SqlAlterMaterializedTableDropPrimaryKey(
                SqlParserPos pos, SqlIdentifier materializedTableName) {
            super(pos, materializedTableName);
        }

        @Override
        public void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword("PRIMARY KEY");
        }
    }

    /**
     * ALTER MATERIALIZED TABLE [catalog_name.][db_name.]materialized_table_name DROP CONSTRAINT
     * constraint_name.
     */
    public static class SqlAlterMaterializedTableDropConstraint
            extends SqlAlterMaterializedTableDropSchema {
        private final SqlIdentifier constraintName;

        public SqlAlterMaterializedTableDropConstraint(
                SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier constraintName) {
            super(pos, tableName);
            this.constraintName = constraintName;
        }

        public SqlIdentifier getConstraintName() {
            return constraintName;
        }

        @Override
        public void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword("CONSTRAINT");
            constraintName.unparse(writer, leftPrec, rightPrec);
        }
    }

    /**
     * SqlNode to describe ALTER MATERIALIZED TABLE materialized_table_name DROP column clause.
     *
     * <p>Example: DDL like the below for drop column.
     *
     * <pre>{@code
     * -- drop single column
     * ALTER MATERIALIZED TABLE prod.db.sample DROP col1;
     *
     * -- drop multiple columns
     * ALTER MATERIALIZED TABLE prod.db.sample DROP (col1, col2, col3);
     * }</pre>
     */
    public static class SqlAlterMaterializedTableDropColumn
            extends SqlAlterMaterializedTableDropSchema {

        private final SqlNodeList columnList;

        public SqlAlterMaterializedTableDropColumn(
                SqlParserPos pos, SqlIdentifier tableName, SqlNodeList columnList) {
            super(pos, tableName);
            this.columnList = columnList;
        }

        public SqlNodeList getColumnList() {
            return columnList;
        }

        @Override
        public List<SqlNode> getOperandList() {
            return List.of(name, columnList);
        }

        @Override
        public void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec) {
            // unparse materialized table column
            SqlUnparseUtils.unparseTableSchema(
                    columnList, List.of(), null, writer, leftPrec, rightPrec);
        }
    }
}

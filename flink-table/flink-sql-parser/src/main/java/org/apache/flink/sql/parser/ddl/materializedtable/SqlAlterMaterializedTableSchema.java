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
 * examples in javadoc for {@link SqlAlterMaterializedTableAddSchema}.
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
}

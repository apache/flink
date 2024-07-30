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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlConstraintValidator;
import org.apache.flink.sql.parser.SqlUnparseUtils;
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

/** Abstract class to describe statements which are used to alter table schema. */
public abstract class SqlAlterTableSchema extends SqlAlterTable implements ExtendedSqlNode {

    protected final SqlNodeList columnList;
    @Nullable protected final SqlWatermark watermark;
    @Nullable protected final SqlDistribution distribution;
    protected final List<SqlTableConstraint> constraints;

    public SqlAlterTableSchema(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> constraints,
            @Nullable SqlWatermark sqlWatermark,
            @Nullable SqlDistribution distribution,
            boolean ifTableExists) {
        super(pos, tableName, ifTableExists);
        this.columnList = columnList;
        this.constraints = constraints;
        this.distribution = distribution;
        this.watermark = sqlWatermark;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                getTableName(),
                columnList,
                new SqlNodeList(constraints, SqlParserPos.ZERO),
                watermark);
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

    public Optional<SqlDistribution> getDistribution() {
        return Optional.ofNullable(distribution);
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

    void unparseSchemaAndDistribution(SqlWriter writer, int leftPrec, int rightPrec) {
        if ((columnList != null && columnList.size() > 0)
                || (constraints != null && constraints.size() > 0)
                || watermark != null) {
            SqlUnparseUtils.unparseTableSchema(
                    writer, leftPrec, rightPrec, columnList, constraints, watermark);
        }
        if (distribution != null) {
            distribution.unparseAlter(writer, leftPrec, rightPrec);
        }
    }
}

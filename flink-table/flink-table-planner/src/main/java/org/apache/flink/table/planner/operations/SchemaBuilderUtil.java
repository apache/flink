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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.table.api.Schema.UnresolvedComputedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** A utility class for {@link MergeTableAsUtil} and {@link MergeTableLikeUtil} classes. */
public class SchemaBuilderUtil {
    protected final SqlValidator sqlValidator;
    protected final Function<SqlNode, String> escapeExpressions;
    protected final DataTypeFactory dataTypeFactory;

    public SchemaBuilderUtil(
            SqlValidator sqlValidator,
            Function<SqlNode, String> escapeExpressions,
            DataTypeFactory dataTypeFactory) {
        this.sqlValidator = sqlValidator;
        this.escapeExpressions = escapeExpressions;
        this.dataTypeFactory = dataTypeFactory;
    }

    /** Converts a {@link SqlRegularColumn} to an {@link UnresolvedPhysicalColumn} object. */
    public UnresolvedPhysicalColumn toUnresolvedPhysicalColumn(SqlRegularColumn column) {
        final String name = column.getName().getSimple();
        final Optional<String> comment = getComment(column);
        final LogicalType logicalType = toLogicalType(toRelDataType(column.getType()));

        return new UnresolvedPhysicalColumn(
                name, fromLogicalToDataType(logicalType), comment.orElse(null));
    }

    /** Converts a {@link SqlComputedColumn} to an {@link UnresolvedComputedColumn} object. */
    public UnresolvedComputedColumn toUnresolvedComputedColumn(
            SqlComputedColumn column, Map<String, RelDataType> accessibleFieldNamesToTypes) {
        final String name = column.getName().getSimple();
        final Optional<String> comment = getComment(column);

        final SqlNode validatedExpr =
                sqlValidator.validateParameterizedExpression(
                        column.getExpr(), accessibleFieldNamesToTypes);

        return new UnresolvedComputedColumn(
                name,
                new SqlCallExpression(escapeExpressions.apply(validatedExpr)),
                comment.orElse(null));
    }

    /** Converts a {@link SqlMetadataColumn} to an {@link UnresolvedMetadataColumn} object. */
    public UnresolvedMetadataColumn toUnresolvedMetadataColumn(SqlMetadataColumn column) {
        final String name = column.getName().getSimple();
        final Optional<String> comment = getComment(column);
        final LogicalType logicalType = toLogicalType(toRelDataType(column.getType()));

        return new UnresolvedMetadataColumn(
                name,
                fromLogicalToDataType(logicalType),
                column.getMetadataAlias().orElse(null),
                column.isVirtual(),
                comment.orElse(null));
    }

    /**
     * Gets the column data type of {@link UnresolvedPhysicalColumn} column and convert it to a
     * {@link LogicalType}.
     */
    public LogicalType getLogicalType(UnresolvedPhysicalColumn column) {
        return dataTypeFactory.createDataType(column.getDataType()).getLogicalType();
    }

    public Optional<String> getComment(SqlTableColumn column) {
        return column.getComment().map(c -> ((SqlLiteral) c).getValueAs(String.class));
    }

    public RelDataType toRelDataType(SqlDataTypeSpec type) {
        boolean nullable = type.getNullable() == null || type.getNullable();
        return type.deriveType(sqlValidator, nullable);
    }
}

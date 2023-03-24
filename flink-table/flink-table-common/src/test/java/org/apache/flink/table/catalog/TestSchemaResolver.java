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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Testing implementation of {@link SchemaResolver}. */
public class TestSchemaResolver implements SchemaResolver {

    private final DataTypeFactory dataTypeFactory = new DataTypeFactoryMock();
    private final Map<String, ResolvedExpression> resolveExpressionTable = new HashMap<>();

    @Override
    public ResolvedSchema resolve(Schema schema) {
        final List<Column> columns = resolveColumns(schema.getColumns());

        final List<WatermarkSpec> watermarkSpecs =
                schema.getWatermarkSpecs().stream()
                        .map(this::resolveWatermarkSpecs)
                        .collect(Collectors.toList());

        final UniqueConstraint primaryKey = resolvePrimaryKey(schema.getPrimaryKey().orElse(null));

        return new ResolvedSchema(columns, watermarkSpecs, primaryKey);
    }

    private List<Column> resolveColumns(List<Schema.UnresolvedColumn> unresolvedColumns) {
        final Column[] resolvedColumns = new Column[unresolvedColumns.size()];

        int i = 0;
        for (Schema.UnresolvedColumn unresolvedColumn : unresolvedColumns) {
            if (unresolvedColumn instanceof Schema.UnresolvedPhysicalColumn) {
                resolvedColumns[i] =
                        resolvePhysicalColumn((Schema.UnresolvedPhysicalColumn) unresolvedColumn);
            } else if (unresolvedColumn instanceof Schema.UnresolvedMetadataColumn) {
                resolvedColumns[i] =
                        resolveMetadataColumn((Schema.UnresolvedMetadataColumn) unresolvedColumn);
            } else if (unresolvedColumn instanceof Schema.UnresolvedComputedColumn) {
                resolvedColumns[i] =
                        Column.computed(
                                        unresolvedColumn.getName(),
                                        resolveExpression(
                                                ((Schema.UnresolvedComputedColumn) unresolvedColumn)
                                                        .getExpression()))
                                .withComment(unresolvedColumn.getComment().orElse(null));
            }
            i++;
        }
        return Arrays.asList(resolvedColumns);
    }

    private Column.PhysicalColumn resolvePhysicalColumn(
            Schema.UnresolvedPhysicalColumn unresolvedColumn) {
        return Column.physical(
                        unresolvedColumn.getName(),
                        dataTypeFactory.createDataType(unresolvedColumn.getDataType()))
                .withComment(unresolvedColumn.getComment().orElse(null));
    }

    private Column.MetadataColumn resolveMetadataColumn(
            Schema.UnresolvedMetadataColumn unresolvedColumn) {
        return Column.metadata(
                        unresolvedColumn.getName(),
                        dataTypeFactory.createDataType(unresolvedColumn.getDataType()),
                        unresolvedColumn.getMetadataKey(),
                        unresolvedColumn.isVirtual())
                .withComment(unresolvedColumn.getComment().orElse(null));
    }

    private WatermarkSpec resolveWatermarkSpecs(
            Schema.UnresolvedWatermarkSpec unresolvedWatermarkSpec) {
        return WatermarkSpec.of(
                unresolvedWatermarkSpec.getColumnName(),
                resolveExpression(unresolvedWatermarkSpec.getWatermarkExpression()));
    }

    private @Nullable UniqueConstraint resolvePrimaryKey(
            @Nullable Schema.UnresolvedPrimaryKey unresolvedPrimaryKey) {
        if (unresolvedPrimaryKey == null) {
            return null;
        }

        return UniqueConstraint.primaryKey(
                unresolvedPrimaryKey.getConstraintName(), unresolvedPrimaryKey.getColumnNames());
    }

    private ResolvedExpression resolveExpression(Expression expression) {
        if (expression instanceof SqlCallExpression) {
            String callString = ((SqlCallExpression) expression).getSqlExpression();
            if (resolveExpressionTable.containsKey(callString)) {
                return resolveExpressionTable.get(callString);
            }
        }
        throw new IllegalArgumentException("Unsupported expression: " + expression);
    }

    public TestSchemaResolver addExpression(
            String callExpression, ResolvedExpression resolvedExpression) {
        ResolvedExpression oldValue =
                resolveExpressionTable.put(callExpression, resolvedExpression);
        if (oldValue != null) {
            throw new IllegalArgumentException("Conflict key for expression: " + callExpression);
        }
        return this;
    }
}

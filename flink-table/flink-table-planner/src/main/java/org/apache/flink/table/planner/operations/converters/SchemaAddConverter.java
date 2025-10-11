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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;

import java.util.Collections;
import java.util.stream.Collectors;

/** Converter for ALTER TABLE ADD ... schema operations. */
public class SchemaAddConverter extends SchemaConverter {

    public SchemaAddConverter(ResolvedCatalogTable oldTable, ConvertContext context) {
        super(oldTable, context);
    }

    @Override
    protected void checkAndCollectPrimaryKeyChange() {
        if (primaryKey != null) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table has already defined the primary key constraint %s. You might "
                                    + "want to drop it before adding a new one.",
                            EX_MSG_PREFIX,
                            primaryKey.getColumnNames().stream()
                                    .collect(Collectors.joining("`, `", "[`", "`]"))));
        }
        changeBuilders.add(
                schema ->
                        Collections.singletonList(TableChange.add(unwrap(schema.getPrimaryKey()))));
    }

    @Override
    protected void checkAndCollectDistributionChange(TableDistribution newDistribution) {
        if (distribution != null) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table has already defined the distribution `%s`. "
                                    + "You can modify it or drop it before adding a new one.",
                            EX_MSG_PREFIX, distribution));
        }
        changesCollector.add(TableChange.add(newDistribution));
    }

    @Override
    protected void checkAndCollectWatermarkChange() {
        if (watermarkSpec != null) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table has already defined the watermark strategy `%s` AS %s. You might "
                                    + "want to drop it before adding a new one.",
                            EX_MSG_PREFIX,
                            watermarkSpec.getColumnName(),
                            ((SqlCallExpression) watermarkSpec.getWatermarkExpression())
                                    .getSqlExpression()));
        }
        changeBuilders.add(
                schema ->
                        Collections.singletonList(
                                TableChange.add(schema.getWatermarkSpecs().get(0))));
    }

    @Override
    protected void updatePositionAndCollectColumnChange(
            SqlTableColumnPosition columnPosition, String columnName) {
        if (sortedColumnNames.contains(columnName)) {
            throw new ValidationException(
                    String.format(
                            "%sTry to add a column `%s` which already exists in the table.",
                            EX_MSG_PREFIX, columnName));
        }

        if (columnPosition.isFirstColumn()) {
            changeBuilders.add(
                    schema ->
                            Collections.singletonList(
                                    TableChange.add(
                                            unwrap(schema.getColumn(columnName)),
                                            TableChange.ColumnPosition.first())));
            sortedColumnNames.add(0, columnName);
        } else if (columnPosition.isAfterReferencedColumn()) {
            String referenceName = getReferencedColumn(columnPosition);
            sortedColumnNames.add(sortedColumnNames.indexOf(referenceName) + 1, columnName);
            changeBuilders.add(
                    schema ->
                            Collections.singletonList(
                                    TableChange.add(
                                            unwrap(schema.getColumn(columnName)),
                                            TableChange.ColumnPosition.after(referenceName))));
        } else {
            changeBuilders.add(
                    schema ->
                            Collections.singletonList(
                                    TableChange.add(unwrap(schema.getColumn(columnName)))));
            sortedColumnNames.add(columnName);
        }
    }
}

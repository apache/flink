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

package org.apache.flink.table.planner.operations.converters.table;

import org.apache.flink.sql.parser.ddl.table.SqlAlterTableDropColumn;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.converters.SchemaReferencesManager;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Convert ALTER TABLE DROP (col1 [, col2, ...]) to generate an updated Schema. */
public class SqlAlterTableDropColumnConverter
        extends AbstractAlterTableConverter<SqlAlterTableDropColumn> {
    @Override
    protected Operation convertToOperation(
            SqlAlterTableDropColumn dropColumn,
            ResolvedCatalogTable oldTable,
            ConvertContext context) {
        Set<String> columnsToDrop = new HashSet<>();
        dropColumn
                .getColumnList()
                .forEach(
                        identifier -> {
                            String name = getColumnName((SqlIdentifier) identifier);
                            if (!columnsToDrop.add(name)) {
                                throw new ValidationException(
                                        String.format(
                                                "%sDuplicate column `%s`.", EX_MSG_PREFIX, name));
                            }
                        });

        SchemaReferencesManager referencesManager = SchemaReferencesManager.create(oldTable);
        // Sort by dependencies count from smallest to largest. For example, when dropping column a,
        // b(b as a+1), the order should be: [b, a] after sort.
        List<String> sortedColumnsToDrop =
                columnsToDrop.stream()
                        .sorted(
                                Comparator.comparingInt(
                                                col ->
                                                        referencesManager.getColumnDependencyCount(
                                                                (String) col))
                                        .reversed())
                        .collect(Collectors.toList());
        List<TableChange> tableChanges = new ArrayList<>(sortedColumnsToDrop.size());
        for (String columnToDrop : sortedColumnsToDrop) {
            referencesManager.dropColumn(columnToDrop, () -> EX_MSG_PREFIX);
            tableChanges.add(TableChange.dropColumn(columnToDrop));
        }

        final Schema schema = getUpdatedSchema(oldTable, columnsToDrop);

        return buildAlterTableChangeOperation(
                dropColumn, tableChanges, schema, oldTable, context.getCatalogManager());
    }

    private Schema getUpdatedSchema(
            ResolvedCatalogBaseTable<?> oldTable, Set<String> columnsToDrop) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        SchemaReferencesManager.buildUpdatedColumn(
                schemaBuilder,
                oldTable,
                (builder, column) -> {
                    if (!columnsToDrop.contains(column.getName())) {
                        builder.fromColumns(Collections.singletonList(column));
                    }
                });
        SchemaReferencesManager.buildUpdatedPrimaryKey(
                schemaBuilder, oldTable, Function.identity());
        SchemaReferencesManager.buildUpdatedWatermark(schemaBuilder, oldTable);
        return schemaBuilder.build();
    }
}

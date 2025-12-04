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

import org.apache.flink.sql.parser.ddl.table.SqlCreateTable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract class for converting {@link SqlCreateTable} and it's children to create table
 * operations.
 */
public abstract class AbstractCreateTableConverter<T extends SqlCreateTable>
        implements SqlNodeConverter<T> {

    /** Context of create table converters while merging source and derived items. */
    protected interface MergeContext {
        Schema getMergedSchema(ResolvedSchema schemaToMerge);

        Map<String, String> getMergedTableOptions();

        List<String> getMergedPartitionKeys();

        Optional<TableDistribution> getMergedTableDistribution();
    }

    protected abstract MergeContext getMergeContext(T sqlCreateTable, ConvertContext context);

    protected final Optional<TableDistribution> getDerivedTableDistribution(T sqlCreateTable) {
        return Optional.ofNullable(sqlCreateTable.getDistribution())
                .map(OperationConverterUtils::getDistributionFromSqlDistribution);
    }

    protected final ResolvedCatalogTable getResolvedCatalogTable(
            T sqlCreateTable, ConvertContext context, ResolvedSchema schemaToMerge) {
        final MergeContext mergeContext = getMergeContext(sqlCreateTable, context);
        final List<String> partitionKeys = mergeContext.getMergedPartitionKeys();
        final Schema schema = mergeContext.getMergedSchema(schemaToMerge);
        verifyPartitioningColumnsExist(schema, partitionKeys);

        final Map<String, String> tableOptions = mergeContext.getMergedTableOptions();
        final TableDistribution distribution =
                mergeContext.getMergedTableDistribution().orElse(null);
        final String comment = sqlCreateTable.getComment();
        final CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(schema)
                        .comment(comment)
                        .distribution(distribution)
                        .options(tableOptions)
                        .partitionKeys(partitionKeys)
                        .build();
        return context.getCatalogManager().resolveCatalogTable(catalogTable);
    }

    protected final ObjectIdentifier getIdentifier(SqlCreateTable node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.getFullName());
        return context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
    }

    protected final CreateTableOperation getCreateTableOperation(
            ObjectIdentifier identifier,
            ResolvedCatalogTable tableWithResolvedSchema,
            T sqlCreateTable) {
        return new CreateTableOperation(
                identifier,
                tableWithResolvedSchema,
                sqlCreateTable.isIfNotExists(),
                sqlCreateTable.isTemporary());
    }

    private void verifyPartitioningColumnsExist(Schema schema, List<String> partitionKeys) {
        final Set<String> columnNames =
                schema.getColumns().stream()
                        .map(Schema.UnresolvedColumn::getName)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        for (String partitionKey : partitionKeys) {
            if (!columnNames.contains(partitionKey)) {
                throw new ValidationException(
                        String.format(
                                "Partition column '%s' not defined in the table schema. Available columns: [%s]",
                                partitionKey,
                                columnNames.stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }
        }
    }
}

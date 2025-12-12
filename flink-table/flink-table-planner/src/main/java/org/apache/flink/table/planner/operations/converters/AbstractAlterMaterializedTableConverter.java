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

import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTable;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.utils.ValidationUtils;

import java.util.Optional;
import java.util.function.Consumer;

/** Abstract converter for {@link SqlAlterMaterializedTable}. */
public abstract class AbstractAlterMaterializedTableConverter<T extends SqlAlterMaterializedTable>
        implements SqlNodeConverter<T> {

    protected static final String EX_MSG_PREFIX =
            "Failed to execute ALTER MATERIALIZED TABLE statement.\n";

    protected abstract Operation convertToOperation(
            T sqlAlterTable, ResolvedCatalogMaterializedTable oldTable, ConvertContext context);

    @Override
    public final Operation convertSqlNode(T sqlAlterMaterializedTable, ConvertContext context) {
        final CatalogManager catalogManager = context.getCatalogManager();
        final ObjectIdentifier materializedTableIdentifier =
                resolveIdentifier(sqlAlterMaterializedTable, context);
        final Optional<ContextResolvedTable> optionalCatalogMaterializedTable =
                catalogManager.getTable(materializedTableIdentifier);

        if (optionalCatalogMaterializedTable.isEmpty()
                || optionalCatalogMaterializedTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format(
                            "Materialized table %s doesn't exist.", materializedTableIdentifier));
        }
        ValidationUtils.validateTableKind(
                optionalCatalogMaterializedTable.get().getTable(),
                TableKind.MATERIALIZED_TABLE,
                "alter materialized table");

        return convertToOperation(
                sqlAlterMaterializedTable,
                optionalCatalogMaterializedTable.get().getResolvedTable(),
                context);
    }

    protected ObjectIdentifier resolveIdentifier(
            SqlAlterMaterializedTable sqlAlterMaterializedTable, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterMaterializedTable.getFullName());
        return context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
    }

    protected CatalogMaterializedTable buildUpdatedMaterializedTable(
            ResolvedCatalogMaterializedTable oldTable,
            Consumer<CatalogMaterializedTable.Builder> consumer) {

        CatalogMaterializedTable.Builder builder =
                CatalogMaterializedTable.newBuilder()
                        .schema(oldTable.getUnresolvedSchema())
                        .comment(oldTable.getComment())
                        .partitionKeys(oldTable.getPartitionKeys())
                        .options(oldTable.getOptions())
                        .originalQuery(oldTable.getOriginalQuery())
                        .expandedQuery(oldTable.getExpandedQuery())
                        .distribution(oldTable.getDistribution().orElse(null))
                        .freshness(oldTable.getDefinitionFreshness())
                        .logicalRefreshMode(oldTable.getLogicalRefreshMode())
                        .refreshMode(oldTable.getRefreshMode())
                        .refreshStatus(oldTable.getRefreshStatus())
                        .refreshHandlerDescription(
                                oldTable.getRefreshHandlerDescription().orElse(null))
                        .serializedRefreshHandler(oldTable.getSerializedRefreshHandler());

        consumer.accept(builder);
        return builder.build();
    }
}

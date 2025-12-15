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

import org.apache.flink.sql.parser.ddl.table.SqlAlterTable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;
import org.apache.flink.table.operations.utils.ValidationUtils;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;
import java.util.Optional;

/** Abstract class for ALTER TABLE converters. */
public abstract class AbstractAlterTableConverter<T extends SqlAlterTable>
        implements SqlNodeConverter<T> {
    protected static final String EX_MSG_PREFIX = "Failed to execute ALTER TABLE statement.\n";

    protected abstract Operation convertToOperation(
            T sqlAlterTable, ResolvedCatalogTable oldTable, ConvertContext context);

    @Override
    public final Operation convertSqlNode(T sqlAlterTable, ConvertContext context) {
        final CatalogManager catalogManager = context.getCatalogManager();
        final ObjectIdentifier tableIdentifier = resolveIdentifier(sqlAlterTable, context);
        final Optional<ContextResolvedTable> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);

        if (optionalCatalogTable.isEmpty() || optionalCatalogTable.get().isTemporary()) {
            if (sqlAlterTable.ifTableExists()) {
                return new NopOperation();
            }
            throw new ValidationException(
                    String.format(
                            "Table %s doesn't exist or is a temporary table.", tableIdentifier));
        }
        ValidationUtils.validateTableKind(
                optionalCatalogTable.get().getTable(),
                CatalogBaseTable.TableKind.TABLE,
                "alter table");

        return convertToOperation(
                sqlAlterTable, optionalCatalogTable.get().getResolvedTable(), context);
    }

    protected final Operation buildAlterTableChangeOperation(
            SqlAlterTable alterTable,
            List<TableChange> tableChanges,
            Schema newSchema,
            ResolvedCatalogTable oldTable,
            CatalogManager catalogManager) {
        final TableDistribution tableDistribution = getTableDistribution(alterTable, oldTable);

        CatalogTable.Builder builder =
                CatalogTable.newBuilder()
                        .schema(newSchema)
                        .comment(oldTable.getComment())
                        .partitionKeys(oldTable.getPartitionKeys())
                        .distribution(tableDistribution)
                        .options(oldTable.getOptions());

        oldTable.getSnapshot().ifPresent(builder::snapshot);

        return new AlterTableChangeOperation(
                catalogManager.qualifyIdentifier(
                        UnresolvedIdentifier.of(alterTable.fullTableName())),
                tableChanges,
                builder.build(),
                alterTable.ifTableExists());
    }

    protected static String getColumnName(SqlIdentifier identifier) {
        if (!identifier.isSimple()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "%sAlter nested row type %s is not supported yet.",
                            EX_MSG_PREFIX, identifier));
        }
        return identifier.getSimple();
    }

    protected final ObjectIdentifier resolveIdentifier(SqlAlterTable node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.fullTableName());
        return context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
    }

    protected TableDistribution getTableDistribution(
            SqlAlterTable alterTable, ResolvedCatalogTable oldTable) {
        return oldTable.getDistribution().orElse(null);
    }
}

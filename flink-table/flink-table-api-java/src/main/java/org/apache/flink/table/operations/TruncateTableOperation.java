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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsTruncate;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.operations.utils.ExecutableOperationUtils;

import java.util.Collections;

/** Operation to describe an {@code TRUNCATE TABLE} statement. */
@Internal
public class TruncateTableOperation implements ExecutableOperation {

    private final ObjectIdentifier tableIdentifier;

    public TruncateTableOperation(ObjectIdentifier tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        if (ctx.isStreamingMode()) {
            throw new TableException(
                    "TRUNCATE TABLE statement is not supported in streaming mode.");
        }

        CatalogManager catalogManager = ctx.getCatalogManager();
        ContextResolvedTable contextResolvedTable = catalogManager.getTableOrError(tableIdentifier);
        CatalogBaseTable catalogBaseTable = contextResolvedTable.getTable();
        if (catalogBaseTable.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
            throw new TableException("TRUNCATE TABLE statement is not supported for view.");
        }

        ResolvedCatalogTable resolvedTable = contextResolvedTable.getResolvedTable();
        ObjectIdentifier objectIdentifier = contextResolvedTable.getIdentifier();
        if (contextResolvedTable.isAnonymous()) {
            throw new TableException(
                    "TRUNCATE TABLE statement is not supported for anonymous table.");
        }

        if (TableFactoryUtil.isLegacyConnectorOptions(
                catalogManager.getCatalog(objectIdentifier.getCatalogName()).orElse(null),
                ctx.getTableConfig(),
                ctx.isStreamingMode(),
                tableIdentifier,
                resolvedTable,
                contextResolvedTable.isTemporary())) {
            throw new TableException(
                    String.format(
                            "Can't perform truncate table operation of the table %s"
                                    + " because the corresponding table sink is the legacy "
                                    + "TableSink."
                                    + " Please implement %s for it.",
                            tableIdentifier, DynamicTableSink.class.getName()));
        }

        DynamicTableSink tableSink =
                ExecutableOperationUtils.createDynamicTableSink(
                        contextResolvedTable.getCatalog().orElse(null),
                        () -> ctx.getModuleManager().getFactory((Module::getTableSinkFactory)),
                        objectIdentifier,
                        resolvedTable,
                        Collections.emptyMap(),
                        ctx.getTableConfig(),
                        ctx.getResourceManager().getUserClassLoader(),
                        contextResolvedTable.isTemporary());
        if (tableSink instanceof SupportsTruncate) {
            SupportsTruncate supportsTruncateTableSink = (SupportsTruncate) tableSink;
            try {
                supportsTruncateTableSink.executeTruncation();
                return TableResultInternal.TABLE_RESULT_OK;
            } catch (Exception e) {
                throw new TableException("Fail to execute TRUNCATE TABLE statement.", e);
            }
        } else {
            throw new TableException(
                    String.format(
                            "TRUNCATE TABLE statement is not supported for the table %s since the table hasn't "
                                    + "implemented the interface %s.",
                            tableIdentifier.asSummaryString(), SupportsTruncate.class.getName()));
        }
    }

    @VisibleForTesting
    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    @Override
    public String asSummaryString() {
        return "TRUNCATE TABLE " + tableIdentifier.toString();
    }
}

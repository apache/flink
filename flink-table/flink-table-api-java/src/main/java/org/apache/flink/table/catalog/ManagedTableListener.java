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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MANAGED;
import static org.apache.flink.table.factories.ManagedTableFactory.discoverManagedTableFactory;

/** The listener for managed table operations. */
@Internal
public class ManagedTableListener {

    private final ClassLoader classLoader;

    private final ReadableConfig config;

    public ManagedTableListener(ClassLoader classLoader, ReadableConfig config) {
        this.classLoader = classLoader;
        this.config = config;
    }

    /** Notify for creating managed table. */
    public ResolvedCatalogBaseTable<?> notifyTableCreation(
            @Nullable Catalog catalog,
            ObjectIdentifier identifier,
            ResolvedCatalogBaseTable<?> table,
            boolean isTemporary,
            boolean ignoreIfExists) {
        if (isNewlyManagedTable(catalog, table)) {
            ResolvedCatalogTable managedTable = createManagedTable(identifier, table, isTemporary);
            discoverManagedTableFactory(classLoader)
                    .onCreateTable(
                            createTableFactoryContext(identifier, managedTable, isTemporary),
                            ignoreIfExists);
            return managedTable;
        }
        return table;
    }

    /** Notify for dropping managed table. */
    public void notifyTableDrop(
            ObjectIdentifier identifier,
            ResolvedCatalogBaseTable<?> table,
            boolean isTemporary,
            boolean ignoreIfNotExists) {
        if (table.getTableKind() == MANAGED) {
            discoverManagedTableFactory(classLoader)
                    .onDropTable(
                            createTableFactoryContext(
                                    identifier, (ResolvedCatalogTable) table, isTemporary),
                            ignoreIfNotExists);
        }
    }

    private boolean isNewlyManagedTable(
            @Nullable Catalog catalog, ResolvedCatalogBaseTable<?> table) {
        if (catalog == null || !catalog.supportsManagedTable()) {
            // catalog not support managed table
            return false;
        }

        if (table.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
            // view is not managed table
            return false;
        }

        if (!StringUtils.isNullOrWhitespaceOnly(
                table.getOptions().get(ConnectorDescriptorValidator.CONNECTOR_TYPE))) {
            // legacy connector is not managed table
            return false;
        }

        if (!StringUtils.isNullOrWhitespaceOnly(
                table.getOptions().get(FactoryUtil.CONNECTOR.key()))) {
            // with connector is not managed table
            return false;
        }

        CatalogBaseTable origin = table.getOrigin();

        if (origin instanceof ConnectorCatalogTable) {
            // ConnectorCatalogTable is not managed table
            return false;
        }

        try {
            origin.getOptions();
        } catch (TableException ignore) {
            // exclude abnormal tables, such as InlineCatalogTable that does not have the options
            return false;
        }

        return true;
    }

    /** Enrich options for creating managed table. */
    private ResolvedCatalogTable createManagedTable(
            ObjectIdentifier identifier, ResolvedCatalogBaseTable<?> table, boolean isTemporary) {
        if (!(table instanceof ResolvedCatalogTable)) {
            throw new UnsupportedOperationException(
                    "Managed table only supports catalog table, unsupported table type: "
                            + table.getClass());
        }
        ResolvedCatalogTable resolvedTable = (ResolvedCatalogTable) table;
        Map<String, String> newOptions =
                discoverManagedTableFactory(classLoader)
                        .enrichOptions(
                                createTableFactoryContext(identifier, resolvedTable, isTemporary));
        CatalogTable newTable =
                CatalogTable.ofManaged(
                        table.getUnresolvedSchema(),
                        table.getComment(),
                        resolvedTable.getPartitionKeys(),
                        newOptions);
        return new ResolvedCatalogTable(newTable, table.getResolvedSchema());
    }

    private DynamicTableFactory.Context createTableFactoryContext(
            ObjectIdentifier identifier, ResolvedCatalogTable table, boolean isTemporary) {
        return new FactoryUtil.DefaultDynamicTableContext(
                identifier, table, config, classLoader, isTemporary);
    }
}

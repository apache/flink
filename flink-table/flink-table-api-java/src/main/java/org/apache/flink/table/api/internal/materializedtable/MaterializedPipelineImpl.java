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

package org.apache.flink.table.api.internal.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.CreateMode;
import org.apache.flink.table.api.MaterializedPipeline;
import org.apache.flink.table.api.MaterializedTable;
import org.apache.flink.table.api.MaterializedTableDescriptor;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperationFactory;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperationFactory.Mode;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Implementation of {@link MaterializedPipeline}. It serializes the query when it is constructed
 * and builds the operation against the catalog state at execute time.
 */
@Internal
public final class MaterializedPipelineImpl implements MaterializedPipeline {

    private final TableEnvironmentInternal tableEnvironment;
    private final String serializedQuery;
    private final ObjectIdentifier identifier;
    private final MaterializedTableDescriptor descriptor;

    public MaterializedPipelineImpl(
            TableEnvironmentInternal tableEnvironment,
            QueryOperation queryOperation,
            ObjectIdentifier identifier,
            MaterializedTableDescriptor descriptor) {
        this.tableEnvironment =
                Preconditions.checkNotNull(tableEnvironment, "Table environment must not be null.");
        this.identifier = Preconditions.checkNotNull(identifier, "Identifier must not be null.");
        this.descriptor = Preconditions.checkNotNull(descriptor, "Descriptor must not be null.");
        this.serializedQuery =
                serializeQuery(
                        Preconditions.checkNotNull(
                                queryOperation, "Query operation must not be null."),
                        tableEnvironment.getCatalogManager());
    }

    private static String serializeQuery(
            QueryOperation queryOperation, CatalogManager catalogManager) {
        try {
            return queryOperation.asSerializableString(catalogManager.getSqlFactory());
        } catch (UnsupportedOperationException | TableException | ValidationException e) {
            final String cause = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            throw new ValidationException(
                    "The query of a materialized table must be expressible in SQL. The refresh job"
                            + " re-runs the stored query text, which rules out, for example,"
                            + " functions that are not registered in a catalog, tables created from"
                            + " a TableDescriptor and tables backed by a DataStream. Cause: "
                            + cause,
                    e);
        }
    }

    @Override
    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public MaterializedTable execute() {
        return execute(CreateMode.createOrAlter());
    }

    @Override
    public MaterializedTable execute(StartMode startMode) {
        return execute(CreateMode.createOrAlter(), startMode);
    }

    @Override
    public MaterializedTable execute(CreateMode mode) {
        Preconditions.checkNotNull(mode, "Create mode must not be null.");
        return submit(mode, null);
    }

    @Override
    public MaterializedTable execute(CreateMode mode, StartMode startMode) {
        Preconditions.checkNotNull(mode, "Create mode must not be null.");
        Preconditions.checkNotNull(startMode, "Start mode must not be null.");
        return submit(mode, startMode);
    }

    private MaterializedTable submit(CreateMode mode, @Nullable StartMode startMode) {
        tableEnvironment.executeInternal(
                buildOperation(toFactoryMode(mode), resolveStartMode(startMode)));
        return new MaterializedTableImpl(identifier);
    }

    private Operation buildOperation(Mode factoryMode, @Nullable StartMode startMode) {
        final CatalogManager catalogManager = tableEnvironment.getCatalogManager();
        if (catalogManager.isTemporaryTable(identifier)) {
            throw new ValidationException(
                    String.format(
                            "Cannot %s materialized table %s, because a temporary table or view with the same identifier exists and would shadow it. Drop the temporary object first.",
                            factoryMode == Mode.CREATE ? "create" : "create or alter",
                            identifier.asSummaryString()));
        }

        // Under CREATE the catalog rejects any permanent object at the path.
        if (factoryMode == Mode.CREATE_OR_ALTER) {
            final Optional<ResolvedCatalogBaseTable<?>> existing =
                    catalogManager.getCatalogBaseTable(identifier);
            if (existing.isPresent() && existing.get().getTableKind() == TableKind.TABLE) {
                throw new ValidationException(
                        String.format(
                                "Cannot create or alter materialized table %s, because a regular table with the same identifier exists. Table#materializeInto does not support regular table conversion.",
                                identifier.asSummaryString()));
            }
        }
        // A query statement parses to exactly one query operation.
        final QueryOperation storedQueryOperation =
                (QueryOperation) tableEnvironment.getParser().parse(serializedQuery).get(0);
        return new MaterializedTableOperationFactory(catalogManager, tableEnvironment.getConfig())
                .build(
                        identifier,
                        factoryMode,
                        new DescriptorMaterializedTableMergeContext(
                                descriptor,
                                storedQueryOperation,
                                serializedQuery,
                                startMode,
                                catalogManager,
                                factoryMode == Mode.CREATE
                                        ? "CREATE MATERIALIZED TABLE"
                                        : "CREATE OR ALTER MATERIALIZED TABLE"));
    }

    private Mode toFactoryMode(CreateMode mode) {
        final Mode factoryMode;
        switch (mode.getKind()) {
            case CREATE_OR_ALTER:
                factoryMode = Mode.CREATE_OR_ALTER;
                break;
            case FAIL_IF_EXISTS:
                factoryMode = Mode.CREATE;
                break;
            default:
                throw new TableException("Unsupported create mode: " + mode);
        }

        return factoryMode;
    }

    private @Nullable StartMode resolveStartMode(@Nullable StartMode startMode) {
        return startMode != null ? startMode : descriptor.getStartMode().orElse(null);
    }

    @Override
    public String toString() {
        return "MaterializedPipeline into '" + identifier.asSummaryString() + "'";
    }
}

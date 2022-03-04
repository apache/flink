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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS;
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedTableJsonSerializer.FIELD_NAME_CATALOG_TABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedTableJsonSerializer.FIELD_NAME_IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogTableJsonSerializer.OPTIONS;

/**
 * JSON deserializer for {@link ContextResolvedTable}.
 *
 * @see ContextResolvedTableJsonSerializer for the reverse operation
 */
@Internal
final class ContextResolvedTableJsonDeserializer extends StdDeserializer<ContextResolvedTable> {
    private static final long serialVersionUID = 1L;

    private static final JsonPointer optionsPointer =
            JsonPointer.compile("/" + FIELD_NAME_CATALOG_TABLE + "/" + OPTIONS);

    ContextResolvedTableJsonDeserializer() {
        super(ContextResolvedTable.class);
    }

    @Override
    public ContextResolvedTable deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        final CatalogPlanRestore planRestoreOption =
                SerdeContext.get(ctx).getConfiguration().get(PLAN_RESTORE_CATALOG_OBJECTS);
        final CatalogManager catalogManager =
                SerdeContext.get(ctx).getFlinkContext().getCatalogManager();
        final ObjectNode objectNode = jsonParser.readValueAsTree();

        // Deserialize the two fields, if available
        final ObjectIdentifier identifier =
                JsonSerdeUtil.deserializeOptionalField(
                                objectNode,
                                FIELD_NAME_IDENTIFIER,
                                ObjectIdentifier.class,
                                jsonParser.getCodec(),
                                ctx)
                        .orElse(null);
        final ResolvedCatalogTable resolvedCatalogTable =
                JsonSerdeUtil.deserializeOptionalField(
                                objectNode,
                                FIELD_NAME_CATALOG_TABLE,
                                ResolvedCatalogTable.class,
                                jsonParser.getCodec(),
                                ctx)
                        .orElse(null);

        if (identifier == null && resolvedCatalogTable == null) {
            throw new TableException(
                    String.format(
                            "The input JSON is invalid because it does neither contain '%s' nor '%s'.",
                            FIELD_NAME_IDENTIFIER, FIELD_NAME_CATALOG_TABLE));
        }

        if (identifier == null) {
            return ContextResolvedTable.anonymous(resolvedCatalogTable);
        }

        final Optional<ContextResolvedTable> contextResolvedTableFromCatalog =
                catalogManager.getTable(identifier);

        // If plan has no catalog table field or no options field,
        // the table is permanent in the catalog and the option is plan all enforced, then fail
        if ((resolvedCatalogTable == null || objectNode.at(optionsPointer).isMissingNode())
                && isPlanEnforced(planRestoreOption)
                && contextResolvedTableFromCatalog
                        .map(ContextResolvedTable::isPermanent)
                        .orElse(false)) {
            throw lookupDisabled(identifier);
        }

        // If we have a schema from the plan and from the catalog, we need to check they match.
        if (contextResolvedTableFromCatalog.isPresent() && resolvedCatalogTable != null) {
            final ResolvedSchema schemaFromPlan = resolvedCatalogTable.getResolvedSchema();
            final ResolvedSchema schemaFromCatalog =
                    contextResolvedTableFromCatalog.get().getResolvedSchema();
            if (!areResolvedSchemasEqual(schemaFromPlan, schemaFromCatalog)) {
                throw schemaNotMatching(identifier, schemaFromPlan, schemaFromCatalog);
            }
        }

        // We use what is stored inside the catalog,
        if (resolvedCatalogTable == null || isLookupForced(planRestoreOption)) {
            return contextResolvedTableFromCatalog.orElseThrow(
                    () -> missingTableFromCatalog(identifier, isLookupForced(planRestoreOption)));
        }

        if (contextResolvedTableFromCatalog.isPresent()) {
            // If no config map is present, then the ContextResolvedTable was serialized with
            // SCHEMA, so we just need to return the catalog query result
            if (objectNode.at(optionsPointer).isMissingNode()) {
                return contextResolvedTableFromCatalog.get();
            }

            return contextResolvedTableFromCatalog
                    .flatMap(ContextResolvedTable::getCatalog)
                    .map(c -> ContextResolvedTable.permanent(identifier, c, resolvedCatalogTable))
                    .orElseGet(
                            () -> ContextResolvedTable.temporary(identifier, resolvedCatalogTable));
        }

        return ContextResolvedTable.temporary(identifier, resolvedCatalogTable);
    }

    private boolean areResolvedSchemasEqual(
            ResolvedSchema schemaFromPlan, ResolvedSchema schemaFromCatalog) {
        // For schema equality we check:
        //  * Columns size and order
        //  * For each column: name, kind (class) and type
        //  * Check partition keys set equality
        final List<Column> columnsFromPlan = schemaFromPlan.getColumns();
        final List<Column> columnsFromCatalog = schemaFromCatalog.getColumns();

        if (columnsFromPlan.size() != columnsFromCatalog.size()) {
            return false;
        }

        for (int i = 0; i < columnsFromPlan.size(); i++) {
            final Column columnFromPlan = columnsFromPlan.get(i);
            final Column columnFromCatalog = columnsFromCatalog.get(i);
            if (!Objects.equals(columnFromPlan.getName(), columnFromCatalog.getName())
                    || !Objects.equals(columnFromPlan.getClass(), columnFromCatalog.getClass())
                    || !Objects.equals(
                            columnFromPlan.getDataType(), columnFromCatalog.getDataType())) {
                return false;
            }
        }

        return Objects.equals(schemaFromPlan.getPrimaryKey(), schemaFromCatalog.getPrimaryKey());
    }

    private boolean isLookupForced(CatalogPlanRestore planRestoreOption) {
        return planRestoreOption == CatalogPlanRestore.IDENTIFIER;
    }

    private boolean isPlanEnforced(CatalogPlanRestore planRestoreOption) {
        return planRestoreOption == CatalogPlanRestore.ALL_ENFORCED;
    }

    static TableException lookupDisabled(ObjectIdentifier objectIdentifier) {
        return new TableException(
                String.format(
                        "The persisted plan does not include all required catalog metadata for table '%s'. "
                                + "However, lookup is disabled because option '%s' = '%s'. "
                                + "Either enable the catalog lookup with '%s' = '%s' / '%s' or "
                                + "regenerate the plan with '%s' = '%s'. "
                                + "Make sure the table is not compiled as a temporary table.",
                        objectIdentifier.asSummaryString(),
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        CatalogPlanRestore.ALL_ENFORCED.name(),
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        CatalogPlanRestore.IDENTIFIER.name(),
                        CatalogPlanRestore.ALL.name(),
                        PLAN_COMPILE_CATALOG_OBJECTS.key(),
                        CatalogPlanCompilation.ALL.name()));
    }

    static TableException schemaNotMatching(
            ObjectIdentifier objectIdentifier,
            ResolvedSchema schemaFromPlan,
            ResolvedSchema schemaFromCatalog) {
        return new TableException(
                String.format(
                        "The schema of table '%s' from the persisted plan does not match the "
                                + "schema loaded from the catalog: '%s' != '%s'. "
                                + "Make sure the table schema in the catalog is still identical.",
                        objectIdentifier.asSummaryString(), schemaFromPlan, schemaFromCatalog));
    }

    static TableException missingTableFromCatalog(
            ObjectIdentifier identifier, boolean forcedLookup) {
        final String initialReason;
        if (forcedLookup) {
            initialReason =
                    String.format(
                            "Cannot resolve table '%s' and catalog lookup is forced because '%s' = '%s'. ",
                            identifier.asSummaryString(),
                            PLAN_RESTORE_CATALOG_OBJECTS.key(),
                            CatalogPlanRestore.IDENTIFIER.name());
        } else {
            initialReason =
                    String.format(
                            "Cannot resolve table '%s' and the persisted plan does not include "
                                    + "all required catalog table metadata. ",
                            identifier.asSummaryString());
        }
        return new TableException(
                initialReason
                        + String.format(
                                "Make sure a registered catalog contains the table when restoring or "
                                        + "the table is available as a temporary table. "
                                        + "Otherwise regenerate the plan with '%s' != '%s' and make "
                                        + "sure the table was not compiled as a temporary table.",
                                PLAN_COMPILE_CATALOG_OBJECTS.key(),
                                CatalogPlanCompilation.IDENTIFIER.name()));
    }
}

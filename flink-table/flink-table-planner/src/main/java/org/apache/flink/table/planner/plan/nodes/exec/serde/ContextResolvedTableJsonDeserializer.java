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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore.IDENTIFIER;
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS;
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedTableJsonSerializer.FIELD_NAME_CATALOG_TABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedTableJsonSerializer.FIELD_NAME_IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogTableJsonSerializer.OPTIONS;

class ContextResolvedTableJsonDeserializer extends StdDeserializer<ContextResolvedTable> {
    private static final long serialVersionUID = 1L;

    public ContextResolvedTableJsonDeserializer() {
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
        ResolvedCatalogTable resolvedCatalogTable =
                JsonSerdeUtil.deserializeOptionalField(
                                objectNode,
                                FIELD_NAME_CATALOG_TABLE,
                                ResolvedCatalogTable.class,
                                jsonParser.getCodec(),
                                ctx)
                        .orElse(null);

        if (identifier == null && resolvedCatalogTable == null) {
            throw new ValidationException(
                    String.format(
                            "The input JSON is invalid because it doesn't contain '%s', nor the '%s'.",
                            FIELD_NAME_IDENTIFIER, FIELD_NAME_CATALOG_TABLE));
        }

        if (identifier == null) {
            if (isLookupForced(planRestoreOption)) {
                throw missingIdentifier();
            }
            return ContextResolvedTable.anonymous(resolvedCatalogTable);
        }

        Optional<ContextResolvedTable> contextResolvedTableFromCatalog =
                isLookupEnabled(planRestoreOption)
                        ? catalogManager.getTable(identifier)
                        : Optional.empty();

        // If we have a schema from the plan and from the catalog, we need to check they match.
        if (contextResolvedTableFromCatalog.isPresent() && resolvedCatalogTable != null) {
            ResolvedSchema schemaFromPlan = resolvedCatalogTable.getResolvedSchema();
            ResolvedSchema schemaFromCatalog =
                    contextResolvedTableFromCatalog.get().getResolvedSchema();
            if (!areResolvedSchemasEqual(schemaFromPlan, schemaFromCatalog)) {
                throw schemaNotMatching(identifier, schemaFromPlan, schemaFromCatalog);
            }
        }

        if (resolvedCatalogTable == null || isLookupForced(planRestoreOption)) {
            if (!isLookupEnabled(planRestoreOption)) {
                throw lookupDisabled(identifier);
            }
            // We use what is stored inside the catalog
            return contextResolvedTableFromCatalog.orElseThrow(
                    () -> missingTableFromCatalog(identifier, isLookupForced(planRestoreOption)));
        }

        if (contextResolvedTableFromCatalog.isPresent()) {
            // If no config map is present, then the ContextResolvedTable was serialized with
            // SCHEMA, so we just need to return the catalog query result
            if (objectNode.at("/" + FIELD_NAME_CATALOG_TABLE + "/" + OPTIONS).isMissingNode()) {
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
        List<Column> columnsFromPlan = schemaFromPlan.getColumns();
        List<Column> columnsFromCatalog = schemaFromCatalog.getColumns();

        if (columnsFromPlan.size() != columnsFromCatalog.size()) {
            return false;
        }

        for (int i = 0; i < columnsFromPlan.size(); i++) {
            Column columnFromPlan = columnsFromPlan.get(i);
            Column columnFromCatalog = columnsFromCatalog.get(i);
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
        return planRestoreOption == IDENTIFIER;
    }

    private boolean isLookupEnabled(CatalogPlanRestore planRestoreOption) {
        return planRestoreOption != CatalogPlanRestore.ALL_ENFORCED;
    }

    static ValidationException missingIdentifier() {
        return new ValidationException(
                String.format(
                        "The table cannot be deserialized, as no identifier is present within the JSON, "
                                + "but lookup is forced by '%s' == '%s'. "
                                + "Either allow restoring table from the catalog with '%s' == '%s' | '%s' or make sure you don't use anonymous tables when generating the plan.",
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        IDENTIFIER.name(),
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        CatalogPlanRestore.ALL.name(),
                        CatalogPlanRestore.ALL_ENFORCED.name()));
    }

    static ValidationException lookupDisabled(ObjectIdentifier objectIdentifier) {
        return new ValidationException(
                String.format(
                        "The table '%s' does not contain any '%s' field, "
                                + "but lookup is disabled because option '%s' == '%s'. "
                                + "Either enable the catalog lookup with '%s' == '%s' | '%s' or regenerate the plan with '%s' != '%s'.",
                        objectIdentifier.asSummaryString(),
                        FIELD_NAME_CATALOG_TABLE,
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        CatalogPlanRestore.ALL_ENFORCED.name(),
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        IDENTIFIER.name(),
                        CatalogPlanRestore.ALL.name(),
                        PLAN_COMPILE_CATALOG_OBJECTS.key(),
                        CatalogPlanCompilation.IDENTIFIER.name()));
    }

    static ValidationException schemaNotMatching(
            ObjectIdentifier objectIdentifier,
            ResolvedSchema schemaFromPlan,
            ResolvedSchema schemaFromCatalog) {
        return new ValidationException(
                String.format(
                        "The schema of the table '%s' from the persisted plan does not match the schema loaded from the catalog: '%s' != '%s'. "
                                + "Have you modified the table schema in the catalog before restoring the plan?.",
                        objectIdentifier.asSummaryString(), schemaFromPlan, schemaFromCatalog));
    }

    static ValidationException missingTableFromCatalog(
            ObjectIdentifier identifier, boolean forcedLookup) {
        String initialReason;
        if (forcedLookup) {
            initialReason =
                    String.format(
                            "Cannot resolve the table '%s' and catalog lookup is forced because '%s' == '%s'. ",
                            identifier.asSummaryString(),
                            PLAN_RESTORE_CATALOG_OBJECTS.key(),
                            IDENTIFIER);
        } else {
            initialReason =
                    String.format(
                            "Cannot resolve the table '%s' and the persisted plan does not include the '%s' field. ",
                            identifier.asSummaryString(), FIELD_NAME_CATALOG_TABLE);
        }
        return new ValidationException(
                initialReason
                        + String.format(
                                "Make sure a registered catalog contains the table when restoring, "
                                        + "or it's not a temporary table, or regenerate the plan with '%s' != '%s'.",
                                PLAN_COMPILE_CATALOG_OBJECTS.key(),
                                CatalogPlanCompilation.IDENTIFIER.name())
                        + ".");
    }
}

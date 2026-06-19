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
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS;
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.areColumnsEqual;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.deserializeOptionalField;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.isLookupForced;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.isPlanEnforced;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.traverse;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedModelJsonSerializer.FIELD_NAME_CATALOG_MODEL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedModelJsonSerializer.FIELD_NAME_IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogModelJsonSerializer.OPTIONS;

/**
 * JSON deserializer for {@link ContextResolvedModel}.
 *
 * @see ContextResolvedModelJsonSerializer for the reverse operation
 */
@Internal
public class ContextResolvedModelJsonDeserializer extends StdDeserializer<ContextResolvedModel> {

    private static final long serialVersionUID = 1L;

    private static final JsonPointer optionsPointer =
            JsonPointer.compile("/" + FIELD_NAME_CATALOG_MODEL + "/" + OPTIONS);

    public ContextResolvedModelJsonDeserializer() {
        super(ContextResolvedModel.class);
    }

    @Override
    public ContextResolvedModel deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        final TableConfigOptions.CatalogPlanRestore planRestoreOption =
                SerdeContext.get(ctx).getConfiguration().get(PLAN_RESTORE_CATALOG_OBJECTS);
        final CatalogManager catalogManager =
                SerdeContext.get(ctx).getFlinkContext().getCatalogManager();
        final ObjectNode objectNode = jsonParser.readValueAsTree();

        final ObjectIdentifier identifier =
                ctx.readValue(
                        traverse(objectNode.required(FIELD_NAME_IDENTIFIER), jsonParser.getCodec()),
                        ObjectIdentifier.class);
        final @Nullable ResolvedCatalogModel restoredModel =
                deserializeOptionalField(
                                objectNode,
                                FIELD_NAME_CATALOG_MODEL,
                                ResolvedCatalogModel.class,
                                jsonParser.getCodec(),
                                ctx)
                        .orElse(null);

        final Optional<ContextResolvedModel> contextResolvedModelFromCatalog =
                catalogManager.getModel(identifier);

        // If plan has no catalog model field or no options field,
        // the model is permanent in the catalog and the option is plan all enforced, then fail
        if ((objectNode.at(optionsPointer).isMissingNode()
                && isPlanEnforced(planRestoreOption)
                && contextResolvedModelFromCatalog
                        .map(ContextResolvedModel::isPermanent)
                        .orElse(false))) {
            throw lookupDisabled(identifier);
        }

        // If we have a schema from the plan and from the catalog, we need to check they match.
        if (restoredModel != null && contextResolvedModelFromCatalog.isPresent()) {
            ContextResolvedModel modelFromCatalog = contextResolvedModelFromCatalog.get();
            if (!areColumnsEqual(
                    restoredModel.getResolvedInputSchema(),
                    modelFromCatalog.getResolvedModel().getResolvedInputSchema())) {
                throw schemaNotMatching(
                        identifier,
                        "input schema",
                        restoredModel.getResolvedInputSchema(),
                        modelFromCatalog.getResolvedModel().getResolvedInputSchema());
            }
            if (!areColumnsEqual(
                    restoredModel.getResolvedOutputSchema(),
                    modelFromCatalog.getResolvedModel().getResolvedOutputSchema())) {
                throw schemaNotMatching(
                        identifier,
                        "output schema",
                        restoredModel.getResolvedInputSchema(),
                        modelFromCatalog.getResolvedModel().getResolvedInputSchema());
            }
        }

        if (restoredModel == null || isLookupForced(planRestoreOption)) {
            return contextResolvedModelFromCatalog.orElseThrow(
                    () -> missingModelFromCatalog(identifier, isLookupForced(planRestoreOption)));
        }

        if (contextResolvedModelFromCatalog.isPresent()) {
            // If no config map is present, then the ContextResolvedModel was serialized with
            // SCHEMA, so we just need to return the catalog query result
            if (objectNode.at(optionsPointer).isMissingNode()) {
                return contextResolvedModelFromCatalog.get();
            }

            return contextResolvedModelFromCatalog
                    .flatMap(ContextResolvedModel::getCatalog)
                    .map(c -> ContextResolvedModel.permanent(identifier, c, restoredModel))
                    .orElseGet(() -> ContextResolvedModel.temporary(identifier, restoredModel));
        }

        return ContextResolvedModel.temporary(identifier, restoredModel);
    }

    static TableException schemaNotMatching(
            ObjectIdentifier objectIdentifier,
            String schemaType,
            ResolvedSchema schemaFromPlan,
            ResolvedSchema schemaFromCatalog) {
        return new TableException(
                String.format(
                        "The %s of model '%s' from the persisted plan does not match the "
                                + "model loaded from the catalog: '%s' != '%s'. "
                                + "Make sure the model %s in the catalog is still identical.",
                        schemaType,
                        objectIdentifier.asSummaryString(),
                        schemaFromPlan,
                        schemaFromCatalog,
                        schemaType));
    }

    static TableException lookupDisabled(ObjectIdentifier objectIdentifier) {
        return new TableException(
                String.format(
                        "The persisted plan does not include all required catalog metadata for model '%s'. "
                                + "However, lookup is disabled because option '%s' = '%s'. "
                                + "Either enable the catalog lookup with '%s' = '%s' / '%s' or "
                                + "regenerate the plan with '%s' = '%s'. "
                                + "Make sure the model is not compiled as a temporary model.",
                        objectIdentifier.asSummaryString(),
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        TableConfigOptions.CatalogPlanRestore.ALL_ENFORCED.name(),
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        TableConfigOptions.CatalogPlanRestore.IDENTIFIER.name(),
                        TableConfigOptions.CatalogPlanRestore.ALL.name(),
                        PLAN_COMPILE_CATALOG_OBJECTS.key(),
                        TableConfigOptions.CatalogPlanCompilation.ALL.name()));
    }

    static TableException missingModelFromCatalog(
            ObjectIdentifier identifier, boolean forcedLookup) {
        String initialReason;
        if (forcedLookup) {
            initialReason =
                    String.format(
                            "Cannot resolve model '%s' and catalog lookup is forced because '%s' = '%s'. ",
                            identifier.asSummaryString(),
                            PLAN_RESTORE_CATALOG_OBJECTS.key(),
                            TableConfigOptions.CatalogPlanRestore.IDENTIFIER.name());
        } else {
            initialReason =
                    String.format(
                            "Cannot resolve model '%s' and the persisted plan does not include "
                                    + "all required catalog model metadata. ",
                            identifier.asSummaryString());
        }
        return new TableException(
                initialReason
                        + String.format(
                                "Make sure a registered catalog contains the model when restoring or "
                                        + "the model is available as a temporary model. "
                                        + "Otherwise regenerate the plan with '%s' != '%s' and make "
                                        + "sure the model was not compiled as a temporary model.",
                                PLAN_COMPILE_CATALOG_OBJECTS.key(),
                                TableConfigOptions.CatalogPlanCompilation.IDENTIFIER.name()));
    }
}

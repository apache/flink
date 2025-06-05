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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.createJsonObjectReader;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.createJsonObjectWriter;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedModelJsonSerializer.FIELD_NAME_CATALOG_MODEL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedModelJsonSerializer.FIELD_NAME_IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.assertThatJsonContains;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.assertThatJsonDoesNotContain;
import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_CATALOG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ContextResolvedModel} serialization and deserialization. */
public class ContextResolvedModelSerdeTest {

    private static final ObjectIdentifier TEMPORARY_MODEL_IDENTIFIER =
            ObjectIdentifier.of(DEFAULT_CATALOG, "db1", "my_temporary_model");
    private static final ObjectIdentifier PERMANENT_MODEL_IDENTIFIER =
            ObjectIdentifier.of(DEFAULT_CATALOG, "db1", "my_permanent_model");

    private static final ResolvedSchema INPUT_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.INT()),
                    Column.physical("b", DataTypes.STRING()));

    private static final ResolvedSchema OUTPUT_SCHEMA =
            ResolvedSchema.of(Column.physical("vector", DataTypes.ARRAY(DataTypes.FLOAT())));

    private static final Map<String, String> MODEL_OPTIONS = new HashMap<>();

    static {
        MODEL_OPTIONS.put("a", "1");
        MODEL_OPTIONS.put("b", "2");
        MODEL_OPTIONS.put("c", "3");
    }

    private static final ResolvedCatalogModel RESOLVED_CATALOG_MODEL =
            ResolvedCatalogModel.of(
                    CatalogModel.of(
                            Schema.newBuilder().fromResolvedSchema(INPUT_SCHEMA).build(),
                            Schema.newBuilder().fromResolvedSchema(OUTPUT_SCHEMA).build(),
                            MODEL_OPTIONS,
                            "This is a model."),
                    INPUT_SCHEMA,
                    OUTPUT_SCHEMA);

    private static final Catalog CATALOG = new GenericInMemoryCatalog(DEFAULT_CATALOG, "db1");

    private static final CatalogManager CATALOG_MANAGER =
            CatalogManagerMocks.createCatalogManager(CATALOG);

    private static final ContextResolvedModel PERMANENT_CATALOG_CONTEXT_RESOLVED_MODEL =
            ContextResolvedModel.permanent(
                    PERMANENT_MODEL_IDENTIFIER, CATALOG, RESOLVED_CATALOG_MODEL);

    private static final ContextResolvedModel PERMANENT_PLAN_CONTEXT_RESOLVED_MODEL =
            ContextResolvedModel.permanent(
                    PERMANENT_MODEL_IDENTIFIER,
                    CATALOG,
                    ResolvedCatalogModel.of(
                            CatalogModel.of(
                                    Schema.newBuilder().fromResolvedSchema(INPUT_SCHEMA).build(),
                                    Schema.newBuilder().fromResolvedSchema(OUTPUT_SCHEMA).build(),
                                    Collections.singletonMap("k", "v"),
                                    "This is a model."),
                            INPUT_SCHEMA,
                            OUTPUT_SCHEMA));

    static {
        CATALOG_MANAGER.createTemporaryModel(
                RESOLVED_CATALOG_MODEL, TEMPORARY_MODEL_IDENTIFIER, true);
        CATALOG_MANAGER.createModel(RESOLVED_CATALOG_MODEL, PERMANENT_MODEL_IDENTIFIER, true);
    }

    @Test
    void testTemporaryModel() throws Exception {
        ContextResolvedModel inputModel =
                CATALOG_MANAGER.getModelOrError(TEMPORARY_MODEL_IDENTIFIER);
        final Tuple2<JsonNode, ContextResolvedModel> result =
                serDe(
                        serdeContext(
                                TableConfigOptions.CatalogPlanCompilation.ALL,
                                TableConfigOptions.CatalogPlanRestore.ALL_ENFORCED),
                        inputModel);

        assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
        assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_MODEL);
        assertThat(result.f1).isEqualTo(inputModel);
    }

    @Test
    void testMissingTemporaryModel() {
        ContextResolvedModel inputModel =
                ContextResolvedModel.temporary(
                        ObjectIdentifier.of(DEFAULT_CATALOG, "db1", "non-exist-model"),
                        RESOLVED_CATALOG_MODEL);
        assertThatThrownBy(
                        () ->
                                serDe(
                                        serdeContext(
                                                TableConfigOptions.CatalogPlanCompilation.SCHEMA,
                                                TableConfigOptions.CatalogPlanRestore.ALL_ENFORCED),
                                        inputModel))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                TableException.class,
                                "Cannot resolve model 'default_catalog.db1.non-exist-model' and the "
                                        + "persisted plan does not include all required catalog model metadata. Make "
                                        + "sure a registered catalog contains the model when restoring or the model is "
                                        + "available as a temporary model. Otherwise regenerate the plan with "
                                        + "'table.plan.compile.catalog-objects' != 'IDENTIFIER' and make sure the model "
                                        + "was not compiled as a temporary model."));
    }

    @Nested
    @DisplayName("Test CatalogPlanCompilation == IDENTIFIER")
    class TestCompileIdentifier {

        @Nested
        @DisplayName("and CatalogPlanRestore == IDENTIFIER")
        class TestRestoreIdentifier {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.IDENTIFIER,
                            TableConfigOptions.CatalogPlanRestore.IDENTIFIER);

            @Test
            void withPermanentModel() throws Exception {
                final Tuple2<JsonNode, ContextResolvedModel> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_MODEL);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_MODEL);
                assertThat(result.f1).isEqualTo(PERMANENT_CATALOG_CONTEXT_RESOLVED_MODEL);
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL")
        class TestRestoreAll {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.IDENTIFIER,
                            TableConfigOptions.CatalogPlanRestore.ALL);

            @Test
            void withPermanentModel() throws Exception {
                final Tuple2<JsonNode, ContextResolvedModel> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_MODEL);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_MODEL);
                assertThat(result.f1).isEqualTo(PERMANENT_CATALOG_CONTEXT_RESOLVED_MODEL);
            }

            @Test
            void withMissingIdentifierInCatalog() throws Exception {
                final SerdeContext serdeCtx =
                        serdeContext(
                                TableConfigOptions.CatalogPlanCompilation.IDENTIFIER,
                                TableConfigOptions.CatalogPlanRestore.ALL);

                final ObjectIdentifier objectIdentifier =
                        ObjectIdentifier.of(DEFAULT_CATALOG, "db1", "some-invalid-model");
                final ContextResolvedModel spec =
                        ContextResolvedModel.permanent(
                                objectIdentifier, CATALOG, RESOLVED_CATALOG_MODEL);

                final byte[] actualSerialized =
                        createJsonObjectWriter(serdeCtx).writeValueAsBytes(spec);

                assertThatThrownBy(
                                () ->
                                        createJsonObjectReader(serdeCtx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedModel.class))
                        .satisfies(
                                anyCauseMatches(
                                        TableException.class,
                                        "Cannot resolve model 'default_catalog.db1.some-invalid-model' and "
                                                + "the persisted plan does not include all required catalog model metadata. "
                                                + "Make sure a registered catalog contains the model when restoring or "
                                                + "the model is available as a temporary model. Otherwise regenerate the plan "
                                                + "with 'table.plan.compile.catalog-objects' != 'IDENTIFIER' and make "
                                                + "sure the model was not compiled as a temporary model."));
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL_ENFORCED")
        class TestRestoreAllEnforced {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.IDENTIFIER,
                            TableConfigOptions.CatalogPlanRestore.ALL_ENFORCED);

            @Test
            void deserializationFail() throws Exception {
                final byte[] actualSerialized =
                        createJsonObjectWriter(ctx)
                                .writeValueAsBytes(PERMANENT_PLAN_CONTEXT_RESOLVED_MODEL);

                assertThatThrownBy(
                                () ->
                                        createJsonObjectReader(ctx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedModel.class))
                        .satisfies(
                                anyCauseMatches(
                                        TableException.class,
                                        "The persisted plan does not include all required catalog metadata "
                                                + "for model 'default_catalog.db1.my_permanent_model'. However, lookup is "
                                                + "disabled because option 'table.plan.restore.catalog-objects' = 'ALL_ENFORCED'. "
                                                + "Either enable the catalog lookup with 'table.plan.restore.catalog-objects' = 'IDENTIFIER' / 'ALL' or regenerate the plan with 'table.plan.compile.catalog-objects' = 'ALL'. "
                                                + "Make sure the model is not compiled as a temporary model."));
            }

            @Test
            void withShadowingTemporaryTable() throws Exception {
                final ContextResolvedModel spec =
                        ContextResolvedModel.permanent(
                                TEMPORARY_MODEL_IDENTIFIER, CATALOG, RESOLVED_CATALOG_MODEL);

                final Tuple2<JsonNode, ContextResolvedModel> result = serDe(ctx, spec);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_MODEL);

                assertThat(result.f1.isTemporary()).isTrue();
                assertThat(result.f1)
                        .isEqualTo(CATALOG_MANAGER.getModelOrError(TEMPORARY_MODEL_IDENTIFIER));
            }
        }
    }

    @Nested
    @DisplayName("Test CatalogPlanCompilation == SCHEMA")
    class TestCompileSchema {

        @Nested
        @DisplayName("and CatalogPlanRestore == IDENTIFIER")
        class TestRestoreIdentifier {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.IDENTIFIER,
                            TableConfigOptions.CatalogPlanRestore.IDENTIFIER);

            @Test
            void withPermanentModel() throws Exception {
                ContextResolvedModel inputModel =
                        CATALOG_MANAGER.getModelOrError(PERMANENT_MODEL_IDENTIFIER);
                final Tuple2<JsonNode, ContextResolvedModel> result = serDe(ctx, inputModel);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_MODEL);
                assertThat(result.f1).isEqualTo(inputModel);
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL")
        class TestRestoreAll {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.IDENTIFIER,
                            TableConfigOptions.CatalogPlanRestore.ALL);

            @Test
            void withPermanentModel() throws Exception {
                ContextResolvedModel inputModel =
                        CATALOG_MANAGER.getModelOrError(PERMANENT_MODEL_IDENTIFIER);
                final Tuple2<JsonNode, ContextResolvedModel> result = serDe(ctx, inputModel);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_MODEL);
                assertThat(result.f1).isEqualTo(inputModel);
            }

            @Test
            void withMissingIdentifierInCatalog() throws Exception {
                final ObjectIdentifier objectIdentifier =
                        ObjectIdentifier.of(DEFAULT_CATALOG, "db1", "some-invalid-model");
                final ContextResolvedModel spec =
                        ContextResolvedModel.permanent(
                                objectIdentifier, CATALOG, RESOLVED_CATALOG_MODEL);

                final byte[] actualSerialized = createJsonObjectWriter(ctx).writeValueAsBytes(spec);

                assertThatThrownBy(
                                () ->
                                        createJsonObjectReader(ctx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedModel.class))
                        .satisfies(
                                anyCauseMatches(
                                        TableException.class,
                                        "Cannot resolve model 'default_catalog.db1.some-invalid-model' "
                                                + "and the persisted plan does not include all required catalog model "
                                                + "metadata. Make sure a registered catalog contains the model when "
                                                + "restoring or the model is available as a temporary model. Otherwise "
                                                + "regenerate the plan with 'table.plan.compile.catalog-objects' != 'IDENTIFIER' "
                                                + "and make sure the model was not compiled as a temporary model."));
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL_ENFORCED")
        class TestRestoreAllEnforced {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.IDENTIFIER,
                            TableConfigOptions.CatalogPlanRestore.ALL_ENFORCED);

            @Test
            void withPermanentModel() {
                ContextResolvedModel inputModel =
                        CATALOG_MANAGER.getModelOrError(PERMANENT_MODEL_IDENTIFIER);
                assertThatThrownBy(() -> serDe(ctx, inputModel))
                        .satisfies(
                                anyCauseMatches(
                                        TableException.class,
                                        "The persisted plan does not include all required catalog "
                                                + "metadata for model 'default_catalog.db1.my_permanent_model'. However, "
                                                + "lookup is disabled because option 'table.plan.restore.catalog-objects' = 'ALL_ENFORCED'. "
                                                + "Either enable the catalog lookup with 'table.plan.restore.catalog-objects' = 'IDENTIFIER' / 'ALL' "
                                                + "or regenerate the plan with 'table.plan.compile.catalog-objects' = 'ALL'. "
                                                + "Make sure the model is not compiled as a temporary model."));
            }
        }
    }

    @Nested
    @DisplayName("Test CatalogPlanCompilation == ALL")
    class TestCompileAll {

        @Nested
        @DisplayName("and CatalogPlanRestore == IDENTIFIER")
        class TestRestoreIdentifier {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.ALL,
                            TableConfigOptions.CatalogPlanRestore.IDENTIFIER);

            @Test
            void withPermanentModel() throws Exception {
                final Tuple2<JsonNode, ContextResolvedModel> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_MODEL);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_MODEL);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_MODEL,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_MODEL,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1).isEqualTo(PERMANENT_CATALOG_CONTEXT_RESOLVED_MODEL);
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL")
        class TestRestoreAll {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.ALL,
                            TableConfigOptions.CatalogPlanRestore.ALL);

            @Test
            void withPermanentModel() throws Exception {
                final Tuple2<JsonNode, ContextResolvedModel> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_MODEL);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_MODEL);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_MODEL,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_MODEL,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1).isEqualTo(PERMANENT_PLAN_CONTEXT_RESOLVED_MODEL);
            }

            @Test
            void withPermanentModelAndMissingIdentifierInCatalog() throws Exception {
                final ObjectIdentifier objectIdentifier =
                        ObjectIdentifier.of(DEFAULT_CATALOG, "db2", "some-nonexistent-table");
                final ContextResolvedModel spec =
                        ContextResolvedModel.permanent(
                                objectIdentifier, CATALOG, RESOLVED_CATALOG_MODEL);

                final Tuple2<JsonNode, ContextResolvedModel> result = serDe(ctx, spec);

                assertThat(result.f1)
                        .isEqualTo(
                                ContextResolvedModel.temporary(
                                        objectIdentifier, spec.getResolvedModel()));
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL_ENFORCED")
        class TestRestoreAllEnforced {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.ALL,
                            TableConfigOptions.CatalogPlanRestore.ALL_ENFORCED);

            @Test
            void withPermanentModel() throws Exception {
                final Tuple2<JsonNode, ContextResolvedModel> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_MODEL);

                assertThatJsonContains(
                        result.f0, ContextResolvedModelJsonSerializer.FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_MODEL);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_MODEL,
                        ResolvedCatalogModelJsonSerializer.OPTIONS);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_MODEL,
                        ResolvedCatalogModelJsonSerializer.COMMENT);
                assertThat(result.f1).isEqualTo(PERMANENT_PLAN_CONTEXT_RESOLVED_MODEL);
            }
        }
    }

    // ---------------------------------------------------------------------------------

    private Tuple2<JsonNode, ContextResolvedModel> serDe(
            SerdeContext serdeCtx, ContextResolvedModel contextResolvedModel) throws Exception {
        final byte[] actualSerialized =
                createJsonObjectWriter(serdeCtx).writeValueAsBytes(contextResolvedModel);

        final ObjectReader objectReader = createJsonObjectReader(serdeCtx);
        final JsonNode middleDeserialized = objectReader.readTree(actualSerialized);
        final ContextResolvedModel actualDeserialized =
                objectReader.readValue(actualSerialized, ContextResolvedModel.class);

        return Tuple2.of(middleDeserialized, actualDeserialized);
    }

    private static SerdeContext serdeContext(
            TableConfigOptions.CatalogPlanCompilation planCompilationOption,
            TableConfigOptions.CatalogPlanRestore planRestoreOption) {
        // Create table options
        final TableConfig tableConfig =
                TableConfig.getDefault()
                        .set(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS, planRestoreOption)
                        .set(
                                TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS,
                                planCompilationOption);

        return JsonSerdeTestUtil.configuredSerdeContext(CATALOG_MANAGER, tableConfig);
    }
}

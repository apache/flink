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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.ExpressionResolverMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedTableJsonSerializer.FIELD_NAME_CATALOG_TABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ContextResolvedTableJsonSerializer.FIELD_NAME_IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.assertThatJsonContains;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.assertThatJsonDoesNotContain;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.createObjectReader;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.createObjectWriter;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.getObjectMapper;
import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_CATALOG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link ContextResolvedTable} serialization and deserialization. */
@Execution(CONCURRENT)
public class ContextResolvedTableSerdeTest {

    // --- Mock data

    private static final ObjectIdentifier TEMPORARY_TABLE_IDENTIFIER =
            ObjectIdentifier.of(DEFAULT_CATALOG, "db1", "my_temporary_table");
    private static final ObjectIdentifier PERMANENT_TABLE_IDENTIFIER =
            ObjectIdentifier.of(DEFAULT_CATALOG, "db1", "my_permanent_table");

    private static final ResolvedSchema CATALOG_TABLE_RESOLVED_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("a", DataTypes.STRING()),
                            Column.physical("b", DataTypes.INT()),
                            Column.physical("c", DataTypes.BOOLEAN())),
                    Collections.emptyList(),
                    null);
    private static final Schema CATALOG_TABLE_SCHEMA =
            Schema.newBuilder().fromResolvedSchema(CATALOG_TABLE_RESOLVED_SCHEMA).build();

    private static final Map<String, String> CATALOG_OPTIONS = new HashMap<>();

    static {
        CATALOG_OPTIONS.put("a", "1");
        CATALOG_OPTIONS.put("b", "2");
        CATALOG_OPTIONS.put("c", "3");
    }

    private static final Map<String, String> PLAN_OPTIONS = new HashMap<>();

    static {
        PLAN_OPTIONS.put("a", "1");
        PLAN_OPTIONS.put("b", "10");
        PLAN_OPTIONS.put("d", "4");
    }

    private static final List<String> PARTITION_KEYS = Collections.singletonList("a");

    private static final ResolvedCatalogTable RESOLVED_CATALOG_TABLE =
            new ResolvedCatalogTable(
                    CatalogTable.of(
                            CATALOG_TABLE_SCHEMA, "my comment", PARTITION_KEYS, CATALOG_OPTIONS),
                    CATALOG_TABLE_RESOLVED_SCHEMA);

    // Mock catalog

    private static final Catalog CATALOG = new GenericInMemoryCatalog(DEFAULT_CATALOG, "db1");
    private static final CatalogManager CATALOG_MANAGER =
            CatalogManagerMocks.preparedCatalogManager()
                    .defaultCatalog(DEFAULT_CATALOG, CATALOG)
                    .build();

    static {
        CATALOG_MANAGER.initSchemaResolver(true, ExpressionResolverMocks.dummyResolver());
        CATALOG_MANAGER.createTable(RESOLVED_CATALOG_TABLE, PERMANENT_TABLE_IDENTIFIER, false);
        CATALOG_MANAGER.createTemporaryTable(
                RESOLVED_CATALOG_TABLE, TEMPORARY_TABLE_IDENTIFIER, false);
    }

    // For each type of tables we mock the "plan" variant and the "catalog" variant, which differ
    // only by options, except for anonymous

    private static final ContextResolvedTable ANONYMOUS_CONTEXT_RESOLVED_TABLE =
            ContextResolvedTable.anonymous(
                    new ResolvedCatalogTable(
                            CatalogTable.of(
                                    CATALOG_TABLE_SCHEMA,
                                    "my comment",
                                    PARTITION_KEYS,
                                    PLAN_OPTIONS),
                            CATALOG_TABLE_RESOLVED_SCHEMA));

    private static final ContextResolvedTable TEMPORARY_CATALOG_CONTEXT_RESOLVED_TABLE =
            CATALOG_MANAGER.getTableOrError(TEMPORARY_TABLE_IDENTIFIER);
    private static final ContextResolvedTable TEMPORARY_PLAN_CONTEXT_RESOLVED_TABLE =
            ContextResolvedTable.temporary(
                    TEMPORARY_TABLE_IDENTIFIER,
                    new ResolvedCatalogTable(
                            CatalogTable.of(
                                    CATALOG_TABLE_SCHEMA,
                                    "my comment",
                                    PARTITION_KEYS,
                                    PLAN_OPTIONS),
                            CATALOG_TABLE_RESOLVED_SCHEMA));

    private static final ContextResolvedTable PERMANENT_CATALOG_CONTEXT_RESOLVED_TABLE =
            CATALOG_MANAGER.getTableOrError(PERMANENT_TABLE_IDENTIFIER);
    private static final ContextResolvedTable PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE =
            ContextResolvedTable.permanent(
                    PERMANENT_TABLE_IDENTIFIER,
                    CATALOG,
                    new ResolvedCatalogTable(
                            CatalogTable.of(
                                    CATALOG_TABLE_SCHEMA,
                                    "my comment",
                                    PARTITION_KEYS,
                                    PLAN_OPTIONS),
                            CATALOG_TABLE_RESOLVED_SCHEMA));

    @Nested
    @DisplayName("Test CatalogPlanCompilation == IDENTIFIER")
    class TestCompileIdentifier {

        @Test
        void withAnonymousTable() {
            assertThatThrownBy(
                            () ->
                                    JsonSerdeTestUtil.toJson(
                                            serdeContext(
                                                    TableConfigOptions.CatalogPlanCompilation
                                                            .IDENTIFIER,
                                                    TableConfigOptions.CatalogPlanRestore.ALL),
                                            ANONYMOUS_CONTEXT_RESOLVED_TABLE))
                    .satisfies(
                            anyCauseMatches(
                                    ValidationException.class, "Cannot serialize anonymous table"));
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == IDENTIFIER")
        class TestRestoreIdentifier {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.IDENTIFIER,
                            TableConfigOptions.CatalogPlanRestore.IDENTIFIER);

            @Test
            void withTemporaryTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, TEMPORARY_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThat(result.f1).isEqualTo(TEMPORARY_CATALOG_CONTEXT_RESOLVED_TABLE);
            }

            @Test
            void withPermanentTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThat(result.f1).isEqualTo(PERMANENT_CATALOG_CONTEXT_RESOLVED_TABLE);
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
            void withTemporaryTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, TEMPORARY_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThat(result.f1).isEqualTo(TEMPORARY_CATALOG_CONTEXT_RESOLVED_TABLE);
            }

            @Test
            void withPermanentTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThat(result.f1).isEqualTo(PERMANENT_CATALOG_CONTEXT_RESOLVED_TABLE);
            }

            @Test
            void withMissingIdentifierInCatalog() throws Exception {
                SerdeContext serdeCtx =
                        serdeContext(
                                TableConfigOptions.CatalogPlanCompilation.IDENTIFIER,
                                TableConfigOptions.CatalogPlanRestore.ALL);

                ObjectIdentifier objectIdentifier =
                        ObjectIdentifier.of(DEFAULT_CATALOG, "db2", "some-invalid-table");
                ContextResolvedTable spec =
                        ContextResolvedTable.permanent(
                                objectIdentifier,
                                CATALOG,
                                new ResolvedCatalogTable(
                                        CatalogTable.of(
                                                CATALOG_TABLE_SCHEMA,
                                                null,
                                                Collections.emptyList(),
                                                PLAN_OPTIONS),
                                        CATALOG_TABLE_RESOLVED_SCHEMA));

                byte[] actualSerialized = createObjectWriter(serdeCtx).writeValueAsBytes(spec);

                assertThatThrownBy(
                                () ->
                                        createObjectReader(serdeCtx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedTable.class))
                        .satisfies(
                                anyCauseMatches(
                                        ValidationException.class,
                                        ContextResolvedTableJsonDeserializer
                                                .missingTableFromCatalog(objectIdentifier, false)
                                                .getMessage()));
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
                byte[] actualSerialized =
                        createObjectWriter(ctx)
                                .writeValueAsBytes(PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatThrownBy(
                                () ->
                                        createObjectReader(ctx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedTable.class))
                        .satisfies(
                                anyCauseMatches(
                                        ValidationException.class,
                                        ContextResolvedTableJsonDeserializer.lookupDisabled(
                                                        PERMANENT_TABLE_IDENTIFIER)
                                                .getMessage()));
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
                            TableConfigOptions.CatalogPlanCompilation.SCHEMA,
                            TableConfigOptions.CatalogPlanRestore.IDENTIFIER);

            @Test
            void withAnonymousTable() throws Exception {
                byte[] actualSerialized =
                        createObjectWriter(ctx).writeValueAsBytes(ANONYMOUS_CONTEXT_RESOLVED_TABLE);

                assertThatThrownBy(
                                () ->
                                        createObjectReader(ctx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedTable.class))
                        .satisfies(
                                anyCauseMatches(
                                        ValidationException.class,
                                        ContextResolvedTableJsonDeserializer.missingIdentifier()
                                                .getMessage()));
            }

            @Test
            void withTemporaryTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, TEMPORARY_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThat(result.f1).isEqualTo(TEMPORARY_CATALOG_CONTEXT_RESOLVED_TABLE);
            }

            @Test
            void withPermanentTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1.isPermanent()).isTrue();
                assertThat(result.f1.getCatalog()).containsSame(CATALOG);
                assertThat(result.f1.getIdentifier()).isEqualTo(PERMANENT_TABLE_IDENTIFIER);
                assertThat(result.f1.getResolvedSchema()).isEqualTo(CATALOG_TABLE_RESOLVED_SCHEMA);
                assertThat(result.f1.getResolvedTable().getOptions()).isEqualTo(CATALOG_OPTIONS);
            }

            @Test
            void withDifferentSchema() throws Exception {
                ResolvedSchema resolvedSchema =
                        new ResolvedSchema(
                                Arrays.asList(
                                        Column.physical("a", DataTypes.STRING()),
                                        Column.physical("b", DataTypes.STRING()),
                                        Column.physical("c", DataTypes.STRING())),
                                Collections.emptyList(),
                                null);
                ContextResolvedTable spec =
                        ContextResolvedTable.permanent(
                                PERMANENT_TABLE_IDENTIFIER,
                                CATALOG,
                                new ResolvedCatalogTable(
                                        CatalogTable.of(
                                                Schema.newBuilder()
                                                        .fromResolvedSchema(resolvedSchema)
                                                        .build(),
                                                "my comment",
                                                PARTITION_KEYS,
                                                PLAN_OPTIONS),
                                        resolvedSchema));

                byte[] actualSerialized = createObjectWriter(ctx).writeValueAsBytes(spec);

                assertThatThrownBy(
                                () ->
                                        createObjectReader(ctx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedTable.class))
                        .satisfies(
                                anyCauseMatches(
                                        ValidationException.class,
                                        ContextResolvedTableJsonDeserializer.schemaNotMatching(
                                                        PERMANENT_TABLE_IDENTIFIER,
                                                        resolvedSchema,
                                                        CATALOG_TABLE_RESOLVED_SCHEMA)
                                                .getMessage()));
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL")
        class TestRestoreAll {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.SCHEMA,
                            TableConfigOptions.CatalogPlanRestore.ALL);

            @Test
            void withAnonymousTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, ANONYMOUS_CONTEXT_RESOLVED_TABLE);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1.isAnonymous()).isTrue();
                assertThat(result.f1.getResolvedSchema()).isEqualTo(CATALOG_TABLE_RESOLVED_SCHEMA);
                assertThat(result.f1.getResolvedTable().getOptions()).isEmpty();
                assertThat(result.f1.getResolvedTable().getComment()).isEmpty();
            }

            @Test
            void withTemporaryTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, TEMPORARY_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThat(result.f1).isEqualTo(TEMPORARY_CATALOG_CONTEXT_RESOLVED_TABLE);
            }

            @Test
            void withPermanentTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1.isPermanent()).isTrue();
                assertThat(result.f1.getIdentifier()).isEqualTo(PERMANENT_TABLE_IDENTIFIER);
                assertThat(result.f1.getResolvedSchema()).isEqualTo(CATALOG_TABLE_RESOLVED_SCHEMA);
                assertThat(result.f1.<ResolvedCatalogTable>getResolvedTable().getPartitionKeys())
                        .isEqualTo(PARTITION_KEYS);
                assertThat(result.f1.getResolvedTable().getOptions())
                        .isEqualTo(RESOLVED_CATALOG_TABLE.getOptions());
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL_ENFORCED")
        class TestRestoreAllEnforced {

            private final SerdeContext ctx =
                    serdeContext(
                            TableConfigOptions.CatalogPlanCompilation.SCHEMA,
                            TableConfigOptions.CatalogPlanRestore.ALL_ENFORCED);

            @Test
            void withAnonymousTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, ANONYMOUS_CONTEXT_RESOLVED_TABLE);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1.isAnonymous()).isTrue();
                assertThat(result.f1.getResolvedSchema()).isEqualTo(CATALOG_TABLE_RESOLVED_SCHEMA);
                assertThat(result.f1.getResolvedTable().getOptions()).isEmpty();
                assertThat(result.f1.getResolvedTable().getComment()).isEmpty();
            }

            @Test
            void withTemporaryTable() throws Exception {
                byte[] actualSerialized =
                        createObjectWriter(ctx)
                                .writeValueAsBytes(TEMPORARY_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatThrownBy(
                                () ->
                                        createObjectReader(ctx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedTable.class))
                        .satisfies(
                                anyCauseMatches(
                                        ValidationException.class,
                                        ContextResolvedTableJsonDeserializer.lookupDisabled(
                                                        TEMPORARY_TABLE_IDENTIFIER)
                                                .getMessage()));
            }

            @Test
            void withPermanentTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonDoesNotContain(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1.isTemporary()).isTrue();
                assertThat(result.f1.getIdentifier()).isEqualTo(PERMANENT_TABLE_IDENTIFIER);
                assertThat(result.f1.getResolvedSchema()).isEqualTo(CATALOG_TABLE_RESOLVED_SCHEMA);
                assertThat(result.f1.<ResolvedCatalogTable>getResolvedTable().getPartitionKeys())
                        .isEqualTo(PARTITION_KEYS);
                assertThat(result.f1.getResolvedTable().getOptions()).isEmpty();
                assertThat(result.f1.getResolvedTable().getComment()).isEmpty();
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
            void withAnonymousTable() throws Exception {
                byte[] actualSerialized =
                        createObjectWriter(ctx).writeValueAsBytes(ANONYMOUS_CONTEXT_RESOLVED_TABLE);

                assertThatThrownBy(
                                () ->
                                        createObjectReader(ctx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedTable.class))
                        .satisfies(
                                anyCauseMatches(
                                        ValidationException.class,
                                        ContextResolvedTableJsonDeserializer.missingIdentifier()
                                                .getMessage()));
            }

            @Test
            void withTemporaryTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, TEMPORARY_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThat(result.f1).isEqualTo(TEMPORARY_CATALOG_CONTEXT_RESOLVED_TABLE);
            }

            @Test
            void withPermanentTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1).isEqualTo(PERMANENT_CATALOG_CONTEXT_RESOLVED_TABLE);
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
            void withAnonymousTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, ANONYMOUS_CONTEXT_RESOLVED_TABLE);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1.<ResolvedCatalogTable>getResolvedTable())
                        .isEqualTo(ANONYMOUS_CONTEXT_RESOLVED_TABLE.getResolvedTable());
            }

            @Test
            void withTemporaryTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, TEMPORARY_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThat(result.f1).isEqualTo(TEMPORARY_CATALOG_CONTEXT_RESOLVED_TABLE);
            }

            @Test
            void withTemporaryTableAndMissingIdentifierInCatalog() throws Exception {
                ObjectIdentifier objectIdentifier =
                        ObjectIdentifier.of(DEFAULT_CATALOG, "db2", "some-nonexistent-table");
                ContextResolvedTable spec =
                        ContextResolvedTable.temporary(
                                objectIdentifier,
                                new ResolvedCatalogTable(
                                        CatalogTable.of(
                                                CATALOG_TABLE_SCHEMA,
                                                "my amazing table",
                                                Collections.emptyList(),
                                                PLAN_OPTIONS),
                                        CATALOG_TABLE_RESOLVED_SCHEMA));
                byte[] actualSerialized = createObjectWriter(ctx).writeValueAsBytes(spec);

                assertThatThrownBy(
                                () ->
                                        createObjectReader(ctx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedTable.class))
                        .satisfies(
                                anyCauseMatches(
                                        ValidationException.class,
                                        ContextResolvedTableJsonDeserializer
                                                .missingTableFromCatalog(objectIdentifier, false)
                                                .getMessage()));
            }

            @Test
            void withPermanentTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1).isEqualTo(PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);
            }

            @Test
            void withPermanentTableAndMissingIdentifierInCatalog() throws Exception {
                ObjectIdentifier objectIdentifier =
                        ObjectIdentifier.of(DEFAULT_CATALOG, "db2", "some-nonexistent-table");
                ContextResolvedTable spec =
                        ContextResolvedTable.permanent(
                                objectIdentifier,
                                CATALOG,
                                new ResolvedCatalogTable(
                                        CatalogTable.of(
                                                CATALOG_TABLE_SCHEMA,
                                                "my amazing table",
                                                Collections.emptyList(),
                                                PLAN_OPTIONS),
                                        CATALOG_TABLE_RESOLVED_SCHEMA));

                Tuple2<JsonNode, ContextResolvedTable> result = serDe(ctx, spec);

                assertThat(result.f1)
                        .isEqualTo(
                                ContextResolvedTable.temporary(
                                        objectIdentifier, spec.getResolvedTable()));
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
            void withAnonymousTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, ANONYMOUS_CONTEXT_RESOLVED_TABLE);

                assertThatJsonDoesNotContain(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1.<ResolvedCatalogTable>getResolvedTable())
                        .isEqualTo(ANONYMOUS_CONTEXT_RESOLVED_TABLE.getResolvedTable());
            }

            @Test
            void withTemporaryTable() throws Exception {
                byte[] actualSerialized =
                        createObjectWriter(ctx)
                                .writeValueAsBytes(TEMPORARY_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatThrownBy(
                                () ->
                                        createObjectReader(ctx)
                                                .readValue(
                                                        actualSerialized,
                                                        ContextResolvedTable.class))
                        .satisfies(
                                anyCauseMatches(
                                        ValidationException.class,
                                        ContextResolvedTableJsonDeserializer.lookupDisabled(
                                                        TEMPORARY_TABLE_IDENTIFIER)
                                                .getMessage()));
            }

            @Test
            void withPermanentTable() throws Exception {
                Tuple2<JsonNode, ContextResolvedTable> result =
                        serDe(ctx, PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE);

                assertThatJsonContains(result.f0, FIELD_NAME_IDENTIFIER);
                assertThatJsonContains(result.f0, FIELD_NAME_CATALOG_TABLE);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.OPTIONS);
                assertThatJsonContains(
                        result.f0,
                        FIELD_NAME_CATALOG_TABLE,
                        ResolvedCatalogTableJsonSerializer.COMMENT);
                assertThat(result.f1)
                        .isEqualTo(
                                ContextResolvedTable.temporary(
                                        PERMANENT_TABLE_IDENTIFIER,
                                        PERMANENT_PLAN_CONTEXT_RESOLVED_TABLE.getResolvedTable()));
            }
        }
    }

    // ---------------------------------------------------------------------------------

    private Tuple2<JsonNode, ContextResolvedTable> serDe(
            SerdeContext serdeCtx, ContextResolvedTable contextResolvedTable) throws Exception {
        // Serialize/Deserialize test
        byte[] actualSerialized =
                createObjectWriter(serdeCtx).writeValueAsBytes(contextResolvedTable);
        JsonNode middleDeserialized = getObjectMapper().readTree(actualSerialized);
        ContextResolvedTable actualDeserialized =
                createObjectReader(serdeCtx)
                        .readValue(actualSerialized, ContextResolvedTable.class);

        return Tuple2.of(middleDeserialized, actualDeserialized);
    }

    private static SerdeContext serdeContext(
            TableConfigOptions.CatalogPlanCompilation planCompilationOption,
            TableConfigOptions.CatalogPlanRestore planRestoreOption) {
        // Create table options
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig
                .getConfiguration()
                .set(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS, planRestoreOption);
        tableConfig
                .getConfiguration()
                .set(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS, planCompilationOption);

        return JsonSerdeTestUtil.configuredSerdeContext(CATALOG_MANAGER, tableConfig);
    }
}

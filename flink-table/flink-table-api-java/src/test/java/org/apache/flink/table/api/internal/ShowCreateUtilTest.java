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

package org.apache.flink.table.api.internal;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.expressions.DefaultSqlFactory;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link ShowCreateUtil}. */
class ShowCreateUtilTest {
    private static final ObjectIdentifier TABLE_IDENTIFIER =
            ObjectIdentifier.of("catalogName", "dbName", "tableName");
    private static final ObjectIdentifier VIEW_IDENTIFIER =
            ObjectIdentifier.of("catalogName", "dbName", "viewName");
    private static final ObjectIdentifier MATERIALIZED_TABLE_IDENTIFIER =
            ObjectIdentifier.of("catalogName", "dbName", "materializedTableName");

    private static final ResolvedSchema ONE_COLUMN_SCHEMA =
            ResolvedSchema.of(Column.physical("id", DataTypes.INT()));

    private static final ResolvedSchema ONE_COLUMN_SCHEMA_WITH_PRIMARY_KEY =
            new ResolvedSchema(
                    List.of(Column.physical("id", DataTypes.INT())),
                    List.of(),
                    UniqueConstraint.primaryKey("pk", List.of("id")),
                    List.of());

    private static final ResolvedSchema TWO_COLUMNS_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.INT()),
                    Column.physical("name", DataTypes.STRING()));

    @ParameterizedTest(name = "{index}: {2}")
    @MethodSource("argsForShowCreateTable")
    void showCreateTable(
            ResolvedCatalogTable resolvedCatalogTable, boolean isTemporary, String expected) {
        final String createTableString =
                ShowCreateUtil.buildShowCreateTableRow(
                        resolvedCatalogTable,
                        TABLE_IDENTIFIER,
                        isTemporary,
                        DefaultSqlFactory.INSTANCE);
        assertThat(createTableString).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{index}: {2}")
    @MethodSource("argsForShowCreateView")
    void showCreateView(
            ResolvedCatalogView resolvedCatalogView, boolean isTemporary, String expected) {
        final String createViewString =
                ShowCreateUtil.buildShowCreateViewRow(
                        resolvedCatalogView, VIEW_IDENTIFIER, isTemporary);
        assertThat(createViewString).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{index}: {2}")
    @MethodSource("argsForShowCreateMaterializedTable")
    void showCreateMaterializedTable(
            ResolvedCatalogMaterializedTable materializedTable,
            boolean isTemporary,
            String expected) {
        final String createMaterializedTableString =
                ShowCreateUtil.buildShowCreateMaterializedTableRow(
                        materializedTable, MATERIALIZED_TABLE_IDENTIFIER, isTemporary);
        assertThat(createMaterializedTableString).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{index}: {1}")
    @MethodSource("argsForShowCreateCatalog")
    void showCreateCatalog(CatalogDescriptor catalogDescriptor, String expected) {
        final String createCatalogString =
                ShowCreateUtil.buildShowCreateCatalogRow(catalogDescriptor);
        assertThat(createCatalogString).isEqualTo(expected);
    }

    private static Collection<Arguments> argsForShowCreateCatalog() {
        Collection<Arguments> argList = new ArrayList<>();
        Map<String, String> options = new HashMap<>();
        options.put("k_a", "v_a");
        options.put("k_b", "v_b");
        options.put("k_c", "v_c");
        final Configuration configuration = Configuration.fromMap(options);
        argList.add(
                Arguments.of(
                        CatalogDescriptor.of("catalogName", configuration),
                        "CREATE CATALOG `catalogName`\n"
                                + "WITH (\n"
                                + "  'k_a' = 'v_a',\n"
                                + "  'k_b' = 'v_b',\n"
                                + "  'k_c' = 'v_c'\n"
                                + ")\n"));

        argList.add(
                Arguments.of(
                        CatalogDescriptor.of("catalogName", configuration)
                                .setComment("Catalog comment"),
                        "CREATE CATALOG `catalogName`\n"
                                + "COMMENT 'Catalog comment'\n"
                                + "WITH (\n"
                                + "  'k_a' = 'v_a',\n"
                                + "  'k_b' = 'v_b',\n"
                                + "  'k_c' = 'v_c'\n"
                                + ")\n"));

        return argList;
    }

    private static Collection<Arguments> argsForShowCreateView() {
        final Collection<Arguments> argList = new ArrayList<>();
        addTemporaryAndPermanent(
                argList,
                createResolvedView(ONE_COLUMN_SCHEMA, "SELECT 1", "SELECT 1", null),
                "CREATE %sVIEW `catalogName`.`dbName`.`viewName` (\n"
                        + "  `id`\n"
                        + ")\n"
                        + "AS SELECT 1\n");

        addTemporaryAndPermanent(
                argList,
                createResolvedView(
                        TWO_COLUMNS_SCHEMA,
                        "SELECT id, name FROM tbl_a",
                        "SELECT id, name FROM `catalogName`.`dbName`.`tbl_a`",
                        "View comment"),
                "CREATE %sVIEW `catalogName`.`dbName`.`viewName` (\n"
                        + "  `id`,\n"
                        + "  `name`\n"
                        + ")\n"
                        + "COMMENT 'View comment'\n"
                        + "AS SELECT id, name FROM `catalogName`.`dbName`.`tbl_a`\n");
        return argList;
    }

    private static Collection<Arguments> argsForShowCreateTable() {
        final Collection<Arguments> argList = new ArrayList<>();
        addTemporaryAndPermanent(
                argList,
                createResolvedTable(
                        ONE_COLUMN_SCHEMA,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        TableDistribution.of(
                                TableDistribution.Kind.HASH, 2, Arrays.asList("key1", "key2")),
                        null),
                "CREATE %sTABLE `catalogName`.`dbName`.`tableName` (\n"
                        + "  `id` INT\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(`key1`, `key2`) INTO 2 BUCKETS\n");

        addTemporaryAndPermanent(
                argList,
                createResolvedTable(
                        ONE_COLUMN_SCHEMA,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        TableDistribution.of(
                                TableDistribution.Kind.RANGE, 2, Arrays.asList("1", "10")),
                        "Table comment"),
                "CREATE %sTABLE `catalogName`.`dbName`.`tableName` (\n"
                        + "  `id` INT\n"
                        + ")\n"
                        + "COMMENT 'Table comment'\n"
                        + "DISTRIBUTED BY RANGE(`1`, `10`) INTO 2 BUCKETS\n");

        final Map<String, String> options = new HashMap<>();
        options.put("option_key_a", "option_value_a");
        options.put("option_key_b", "option_value_b");
        options.put("option_key_c", "option_value_c");

        addTemporaryAndPermanent(
                argList,
                createResolvedTable(
                        TWO_COLUMNS_SCHEMA,
                        options,
                        Collections.emptyList(),
                        null,
                        "Another table comment"),
                "CREATE %sTABLE `catalogName`.`dbName`.`tableName` (\n"
                        + "  `id` INT,\n"
                        + "  `name` VARCHAR(2147483647)\n"
                        + ")\n"
                        + "COMMENT 'Another table comment'\n"
                        + "WITH (\n"
                        + "  'option_key_a' = 'option_value_a',\n"
                        + "  'option_key_b' = 'option_value_b',\n"
                        + "  'option_key_c' = 'option_value_c'\n"
                        + ")\n");

        addTemporaryAndPermanent(
                argList,
                createResolvedTable(
                        ONE_COLUMN_SCHEMA,
                        Collections.emptyMap(),
                        Arrays.asList("key1", "key2"),
                        null,
                        "comment"),
                "CREATE %sTABLE `catalogName`.`dbName`.`tableName` (\n"
                        + "  `id` INT\n"
                        + ")\n"
                        + "COMMENT 'comment'\n"
                        + "PARTITIONED BY (`key1`, `key2`)\n");

        addTemporaryAndPermanent(
                argList,
                createResolvedTable(
                        TWO_COLUMNS_SCHEMA,
                        options,
                        Arrays.asList("key1", "key2"),
                        TableDistribution.of(
                                TableDistribution.Kind.UNKNOWN, 3, Arrays.asList("1", "2", "3")),
                        "table comment"),
                "CREATE %sTABLE `catalogName`.`dbName`.`tableName` (\n"
                        + "  `id` INT,\n"
                        + "  `name` VARCHAR(2147483647)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "DISTRIBUTED BY (`1`, `2`, `3`) INTO 3 BUCKETS\n"
                        + "PARTITIONED BY (`key1`, `key2`)\n"
                        + "WITH (\n"
                        + "  'option_key_a' = 'option_value_a',\n"
                        + "  'option_key_b' = 'option_value_b',\n"
                        + "  'option_key_c' = 'option_value_c'\n"
                        + ")\n");
        return argList;
    }

    private static Collection<Arguments> argsForShowCreateMaterializedTable() {
        final Collection<Arguments> argList = new ArrayList<>();
        addTemporaryAndPermanent(
                argList,
                createResolvedMaterialized(
                        ONE_COLUMN_SCHEMA,
                        null,
                        List.of(),
                        null,
                        IntervalFreshness.ofMinute("1"),
                        RefreshMode.CONTINUOUS,
                        "SELECT 1"),
                "CREATE %sMATERIALIZED TABLE `catalogName`.`dbName`.`materializedTableName`\n"
                        + "FRESHNESS = INTERVAL '1' MINUTE\n"
                        + "REFRESH_MODE = CONTINUOUS\n"
                        + "AS SELECT 1\n");

        addTemporaryAndPermanent(
                argList,
                createResolvedMaterialized(
                        ONE_COLUMN_SCHEMA_WITH_PRIMARY_KEY,
                        null,
                        List.of(),
                        null,
                        IntervalFreshness.ofMinute("1"),
                        RefreshMode.CONTINUOUS,
                        "SELECT 1"),
                "CREATE %sMATERIALIZED TABLE `catalogName`.`dbName`.`materializedTableName` (\n"
                        + "  CONSTRAINT `pk` PRIMARY KEY (`id`) NOT ENFORCED\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '1' MINUTE\n"
                        + "REFRESH_MODE = CONTINUOUS\n"
                        + "AS SELECT 1\n");

        addTemporaryAndPermanent(
                argList,
                createResolvedMaterialized(
                        TWO_COLUMNS_SCHEMA,
                        "Materialized table comment",
                        List.of("id"),
                        TableDistribution.of(TableDistribution.Kind.HASH, 5, List.of("id")),
                        IntervalFreshness.ofMinute("3"),
                        RefreshMode.FULL,
                        "SELECT id, name FROM tbl_a"),
                "CREATE %sMATERIALIZED TABLE `catalogName`.`dbName`.`materializedTableName`\n"
                        + "COMMENT 'Materialized table comment'\n"
                        + "DISTRIBUTED BY HASH(`id`) INTO 5 BUCKETS\n"
                        + "PARTITIONED BY (`id`)\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT id, name FROM tbl_a\n");

        return argList;
    }

    private static void addTemporaryAndPermanent(
            Collection<Arguments> argList, CatalogBaseTable catalogBaseTable, String sql) {
        argList.add(Arguments.of(catalogBaseTable, false, String.format(sql, "")));
        argList.add(Arguments.of(catalogBaseTable, true, String.format(sql, "TEMPORARY ")));
    }

    private static ResolvedCatalogTable createResolvedTable(
            ResolvedSchema resolvedSchema,
            Map<String, String> options,
            List<String> partitionKeys,
            TableDistribution tableDistribution,
            String comment) {
        CatalogTable.Builder tableBuilder =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .options(options)
                        .comment(comment)
                        .partitionKeys(partitionKeys);
        if (tableDistribution != null) {
            tableBuilder.distribution(tableDistribution);
        }
        return new ResolvedCatalogTable(tableBuilder.build(), resolvedSchema);
    }

    private static ResolvedCatalogView createResolvedView(
            ResolvedSchema resolvedSchema,
            String originalQuery,
            String expandedQuery,
            String comment) {
        return new ResolvedCatalogView(
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        comment,
                        originalQuery,
                        expandedQuery,
                        Collections.emptyMap()),
                resolvedSchema);
    }

    private static ResolvedCatalogMaterializedTable createResolvedMaterialized(
            ResolvedSchema resolvedSchema,
            String comment,
            List<String> partitionBy,
            TableDistribution distribution,
            IntervalFreshness freshness,
            RefreshMode refreshMode,
            String definitionQuery) {
        return new ResolvedCatalogMaterializedTable(
                CatalogMaterializedTable.newBuilder()
                        .comment(comment)
                        .partitionKeys(partitionBy)
                        .distribution(distribution)
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .freshness(freshness)
                        .refreshMode(refreshMode)
                        .definitionQuery(definitionQuery)
                        .logicalRefreshMode(LogicalRefreshMode.AUTOMATIC)
                        .refreshStatus(RefreshStatus.ACTIVATED)
                        .build(),
                resolvedSchema);
    }
}

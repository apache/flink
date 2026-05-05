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
import org.apache.flink.table.catalog.ImmutableColumnsConstraint;
import org.apache.flink.table.catalog.Interval;
import org.apache.flink.table.catalog.Interval.TimeUnit;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.StartMode.StartModeKind;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.expressions.DefaultSqlFactory;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link ShowCreateUtil}. */
class ShowCreateUtilTest {
    private static final ObjectIdentifier TABLE_IDENTIFIER =
            ObjectIdentifier.of("catalogName", "dbName", "tableName");
    private static final ObjectIdentifier VIEW_IDENTIFIER =
            ObjectIdentifier.of("catalogName", "dbName", "viewName");
    private static final ObjectIdentifier MATERIALIZED_TABLE_IDENTIFIER =
            ObjectIdentifier.of("catalogName", "dbName", "materializedTableName");

    private static final Pattern START_MODE_EVALUATED_TIMESTAMP =
            Pattern.compile(
                    "/\\* Evaluated to FROM_TIMESTAMP\\(TIMESTAMP '[^']*'\\) at execution \\*/");

    private static final ResolvedSchema ONE_COLUMN_SCHEMA =
            ResolvedSchema.of(Column.physical("id", DataTypes.INT()));

    private static final ResolvedSchema ONE_COLUMN_SCHEMA_WITH_PRIMARY_KEY =
            new ResolvedSchema(
                    List.of(
                            Column.physical("id", DataTypes.INT()),
                            Column.metadata("mt_column", DataTypes.STRING(), null, true)),
                    List.of(),
                    UniqueConstraint.primaryKey("pk", List.of("id")),
                    List.of(),
                    null);

    private static final ResolvedSchema TWO_COLUMNS_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.INT()),
                    Column.physical("name", DataTypes.STRING()));

    private static final ResolvedSchema TWO_COLUMNS_SCHEMA_WITH_PRIMARY_KEY_AND_IMMUTABLE_COLS =
            new ResolvedSchema(
                    List.of(
                            Column.physical("id", DataTypes.INT()),
                            Column.physical("name", DataTypes.STRING())),
                    List.of(),
                    UniqueConstraint.primaryKey("pk", List.of("id")),
                    List.of(),
                    ImmutableColumnsConstraint.immutableColumns("imt", List.of("name")));

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
            boolean createOrAlter,
            String expected) {
        final String createMaterializedTableString =
                ShowCreateUtil.buildShowCreateMaterializedTableRow(
                        materializedTable,
                        MATERIALIZED_TABLE_IDENTIFIER,
                        false,
                        createOrAlter,
                        ZoneOffset.UTC,
                        DefaultSqlFactory.INSTANCE);
        final String fixedTimestamp = "1970-01-02 12:34:56";
        final String normalizedMTString =
                setFixedTimestamp(createMaterializedTableString, fixedTimestamp);
        final String normalizedExpected = setFixedTimestamp(expected, fixedTimestamp);
        assertThat(normalizedMTString).isEqualTo(normalizedExpected);
    }

    @ParameterizedTest(name = "includeFreshness={0}, includeRefreshMode={1}")
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    void showCreateMaterializedTableWithFlags(
            final boolean includeFreshness, final boolean includeRefreshMode) {
        final ResolvedCatalogMaterializedTable materializedTable =
                createResolvedMaterialized(
                        ONE_COLUMN_SCHEMA,
                        null,
                        List.of(),
                        null,
                        StartMode.of(StartModeKind.FROM_BEGINNING),
                        IntervalFreshness.ofMinute(2),
                        RefreshMode.CONTINUOUS,
                        "SELECT 1",
                        "SELECT 1");
        final String result =
                ShowCreateUtil.buildShowCreateMaterializedTableRow(
                        materializedTable,
                        MATERIALIZED_TABLE_IDENTIFIER,
                        false,
                        false,
                        ZoneOffset.UTC,
                        DefaultSqlFactory.INSTANCE,
                        includeFreshness,
                        includeRefreshMode);

        final StringBuilder expected =
                new StringBuilder()
                        .append(
                                "CREATE MATERIALIZED TABLE `catalogName`.`dbName`.`materializedTableName` (\n")
                        .append("  `id` INT\n")
                        .append(")\n")
                        .append("START_MODE = FROM_BEGINNING\n");
        if (includeFreshness) {
            expected.append("FRESHNESS = INTERVAL '2' MINUTE\n");
        }
        if (includeRefreshMode) {
            expected.append("REFRESH_MODE = CONTINUOUS\n");
        }
        expected.append("AS SELECT 1\n");

        assertThat(result).isEqualTo(expected.toString());
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
                        Map.of(),
                        List.of(),
                        TableDistribution.of(
                                TableDistribution.Kind.HASH, 2, List.of("key1", "key2")),
                        null),
                "CREATE %sTABLE `catalogName`.`dbName`.`tableName` (\n"
                        + "  `id` INT\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(`key1`, `key2`) INTO 2 BUCKETS\n");

        addTemporaryAndPermanent(
                argList,
                createResolvedTable(
                        ONE_COLUMN_SCHEMA,
                        Map.of(),
                        List.of(),
                        TableDistribution.of(TableDistribution.Kind.RANGE, 2, List.of("1", "10")),
                        "Table comment"),
                "CREATE %sTABLE `catalogName`.`dbName`.`tableName` (\n"
                        + "  `id` INT\n"
                        + ")\n"
                        + "COMMENT 'Table comment'\n"
                        + "DISTRIBUTED BY RANGE(`1`, `10`) INTO 2 BUCKETS\n");

        addTemporaryAndPermanent(
                argList,
                createResolvedTable(
                        TWO_COLUMNS_SCHEMA_WITH_PRIMARY_KEY_AND_IMMUTABLE_COLS,
                        Map.of(),
                        List.of(),
                        TableDistribution.of(TableDistribution.Kind.RANGE, 2, List.of("1", "10")),
                        "Table comment"),
                "CREATE %sTABLE `catalogName`.`dbName`.`tableName` (\n"
                        + "  `id` INT,\n"
                        + "  `name` VARCHAR(2147483647),\n"
                        + "  CONSTRAINT `pk` PRIMARY KEY (`id`) NOT ENFORCED\n"
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
                        TWO_COLUMNS_SCHEMA, options, List.of(), null, "Another table comment"),
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
                        ONE_COLUMN_SCHEMA, Map.of(), List.of("key1", "key2"), null, "comment"),
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
                        List.of("key1", "key2"),
                        TableDistribution.of(
                                TableDistribution.Kind.UNKNOWN, 3, List.of("1", "2", "3")),
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
        addCreateAndCreateOrAlter(
                argList,
                createResolvedMaterialized(
                        ONE_COLUMN_SCHEMA,
                        null,
                        List.of(),
                        null,
                        StartMode.of(StartModeKind.FROM_NOW, Interval.of(3, TimeUnit.MINUTE)),
                        IntervalFreshness.ofMinute(1),
                        RefreshMode.CONTINUOUS,
                        "SELECT 1",
                        "SELECT 1"),
                "%sMATERIALIZED TABLE `catalogName`.`dbName`.`materializedTableName` (\n"
                        + "  `id` INT\n"
                        + ")\n"
                        + "START_MODE = FROM_NOW(INTERVAL '3' MINUTE) /* Evaluated to FROM_TIMESTAMP(TIMESTAMP '2020-12-12 23:21:12') at execution */\n"
                        + "FRESHNESS = INTERVAL '1' MINUTE\n"
                        + "REFRESH_MODE = CONTINUOUS\n"
                        + "AS SELECT 1\n");

        addCreateAndCreateOrAlter(
                argList,
                createResolvedMaterialized(
                        ONE_COLUMN_SCHEMA_WITH_PRIMARY_KEY,
                        null,
                        List.of(),
                        null,
                        StartMode.of(StartModeKind.FROM_BEGINNING),
                        IntervalFreshness.ofMinute(1),
                        RefreshMode.CONTINUOUS,
                        "SELECT 1",
                        "SELECT 1"),
                "%sMATERIALIZED TABLE `catalogName`.`dbName`.`materializedTableName` (\n"
                        + "  `id` INT,\n"
                        + "  `mt_column` VARCHAR(2147483647) METADATA VIRTUAL,\n"
                        + "  CONSTRAINT `pk` PRIMARY KEY (`id`) NOT ENFORCED\n"
                        + ")\n"
                        + "START_MODE = FROM_BEGINNING\n"
                        + "FRESHNESS = INTERVAL '1' MINUTE\n"
                        + "REFRESH_MODE = CONTINUOUS\n"
                        + "AS SELECT 1\n");

        addCreateAndCreateOrAlter(
                argList,
                createResolvedMaterialized(
                        TWO_COLUMNS_SCHEMA_WITH_PRIMARY_KEY_AND_IMMUTABLE_COLS,
                        null,
                        List.of(),
                        null,
                        StartMode.of(
                                StartModeKind.FROM_TIMESTAMP, Instant.ofEpochSecond(1740000000)),
                        IntervalFreshness.ofMinute(1),
                        RefreshMode.CONTINUOUS,
                        "SELECT 1",
                        "SELECT 1"),
                "%sMATERIALIZED TABLE `catalogName`.`dbName`.`materializedTableName` (\n"
                        + "  `id` INT,\n"
                        + "  `name` VARCHAR(2147483647),\n"
                        + "  CONSTRAINT `pk` PRIMARY KEY (`id`) NOT ENFORCED\n"
                        + ")\n"
                        + "START_MODE = FROM_TIMESTAMP(TIMESTAMP '2025-02-19 21:20:00')\n"
                        + "FRESHNESS = INTERVAL '1' MINUTE\n"
                        + "REFRESH_MODE = CONTINUOUS\n"
                        + "AS SELECT 1\n");

        addCreateAndCreateOrAlter(
                argList,
                createResolvedMaterialized(
                        TWO_COLUMNS_SCHEMA,
                        "Materialized table comment",
                        List.of("id"),
                        TableDistribution.of(TableDistribution.Kind.HASH, 5, List.of("id")),
                        StartMode.of(
                                StartModeKind.FROM_TIMESTAMP,
                                Instant.ofEpochSecond(1740000000),
                                true),
                        IntervalFreshness.ofMinute(3),
                        RefreshMode.FULL,
                        "SELECT id, name FROM tbl_a",
                        "SELECT id, name FROM `catalogName`.`dbName`.`tbl_a`"),
                "%sMATERIALIZED TABLE `catalogName`.`dbName`.`materializedTableName` (\n"
                        + "  `id` INT,\n"
                        + "  `name` VARCHAR(2147483647)\n"
                        + ")\n"
                        + "COMMENT 'Materialized table comment'\n"
                        + "DISTRIBUTED BY HASH(`id`) INTO 5 BUCKETS\n"
                        + "PARTITIONED BY (`id`)\n"
                        + "START_MODE = FROM_TIMESTAMP(TIMESTAMP WITH LOCAL TIME ZONE '2025-02-19 21:20:00')\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT id, name FROM `catalogName`.`dbName`.`tbl_a`\n");

        addCreateAndCreateOrAlter(
                argList,
                createResolvedMaterialized(
                        TWO_COLUMNS_SCHEMA,
                        "Materialized table comment",
                        List.of("id"),
                        TableDistribution.of(TableDistribution.Kind.HASH, 5, List.of("id")),
                        StartMode.of(
                                StartModeKind.FROM_NOW,
                                Interval.of(Period.of(0, 1, 2), TimeUnit.MONTH)),
                        IntervalFreshness.ofMinute(3),
                        RefreshMode.FULL,
                        "SELECT * FROM tbl_a",
                        "SELECT id, name FROM `catalogName`.`dbName`.`tbl_a`"),
                "%sMATERIALIZED TABLE `catalogName`.`dbName`.`materializedTableName` (\n"
                        + "  `id` INT,\n"
                        + "  `name` VARCHAR(2147483647)\n"
                        + ")\n"
                        + "COMMENT 'Materialized table comment'\n"
                        + "DISTRIBUTED BY HASH(`id`) INTO 5 BUCKETS\n"
                        + "PARTITIONED BY (`id`)\n"
                        + "START_MODE = FROM_NOW(INTERVAL '1' MONTH) /* Evaluated to FROM_TIMESTAMP(TIMESTAMP '1970-01-02 12:34:56') at execution */\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT id, name FROM `catalogName`.`dbName`.`tbl_a`\n");

        return argList;
    }

    private static void addTemporaryAndPermanent(
            Collection<Arguments> argList, CatalogBaseTable catalogBaseTable, String sql) {
        argList.add(Arguments.of(catalogBaseTable, false, String.format(sql, "")));
        argList.add(Arguments.of(catalogBaseTable, true, String.format(sql, "TEMPORARY ")));
    }

    private static void addCreateAndCreateOrAlter(
            Collection<Arguments> argList, CatalogBaseTable catalogBaseTable, String sql) {
        argList.add(Arguments.of(catalogBaseTable, false, String.format(sql, "CREATE ")));
        argList.add(Arguments.of(catalogBaseTable, true, String.format(sql, "CREATE OR ALTER ")));
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
                        Map.of()),
                resolvedSchema);
    }

    private static ResolvedCatalogMaterializedTable createResolvedMaterialized(
            ResolvedSchema resolvedSchema,
            String comment,
            List<String> partitionBy,
            TableDistribution distribution,
            StartMode startMode,
            IntervalFreshness freshness,
            RefreshMode refreshMode,
            String originalQuery,
            String expandedQuery) {
        return new ResolvedCatalogMaterializedTable(
                CatalogMaterializedTable.newBuilder()
                        .comment(comment)
                        .partitionKeys(partitionBy)
                        .distribution(distribution)
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .freshness(freshness)
                        .refreshMode(refreshMode)
                        .originalQuery(originalQuery)
                        .expandedQuery(expandedQuery)
                        .logicalRefreshMode(LogicalRefreshMode.AUTOMATIC)
                        .refreshStatus(RefreshStatus.ACTIVATED)
                        .build(),
                resolvedSchema,
                refreshMode,
                freshness,
                startMode);
    }

    private static String setFixedTimestamp(String sql, String fixedTimestamp) {
        return START_MODE_EVALUATED_TIMESTAMP
                .matcher(sql)
                .replaceAll(
                        "/* Evaluated to FROM_TIMESTAMP(TIMESTAMP '"
                                + fixedTimestamp
                                + "') at execution */");
    }
}

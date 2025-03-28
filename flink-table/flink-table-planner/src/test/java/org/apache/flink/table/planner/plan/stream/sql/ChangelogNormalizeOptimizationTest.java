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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Plan tests for removal of redundant changelog normalize. */
public class ChangelogNormalizeOptimizationTest extends TableTestBase {

    private final JavaStreamTableTestUtil util = javaStreamTestUtil();

    static List<TestSpec> getTests() {
        return Arrays.asList(
                TestSpec.select(SourceTable.UPSERT_SOURCE_PARTIAL_DELETES, SinkTable.UPSERT_SINK),
                TestSpec.select(SourceTable.UPSERT_SOURCE_FULL_DELETES, SinkTable.UPSERT_SINK),
                TestSpec.select(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES,
                        SinkTable.UPSERT_SINK_FULL_DELETES),
                TestSpec.select(
                        SourceTable.UPSERT_SOURCE_FULL_DELETES, SinkTable.UPSERT_SINK_FULL_DELETES),
                TestSpec.select(SourceTable.UPSERT_SOURCE_PARTIAL_DELETES, SinkTable.RETRACT_SINK),
                TestSpec.select(SourceTable.UPSERT_SOURCE_FULL_DELETES, SinkTable.RETRACT_SINK),
                TestSpec.selectWithFilter(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES, SinkTable.UPSERT_SINK),
                TestSpec.selectWithFilter(
                        SourceTable.UPSERT_SOURCE_FULL_DELETES, SinkTable.UPSERT_SINK),
                TestSpec.selectWithFilter(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES, SinkTable.RETRACT_SINK),
                TestSpec.selectWithFilter(
                        SourceTable.UPSERT_SOURCE_FULL_DELETES, SinkTable.RETRACT_SINK),
                TestSpec.join(
                        SourceTable.UPSERT_SOURCE_FULL_DELETES,
                        SourceTable.UPSERT_SOURCE_FULL_DELETES,
                        SinkTable.UPSERT_SINK),
                TestSpec.join(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES,
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES,
                        SinkTable.UPSERT_SINK),
                TestSpec.join(
                        SourceTable.UPSERT_SOURCE_FULL_DELETES,
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES,
                        SinkTable.UPSERT_SINK),
                TestSpec.join(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES,
                        SourceTable.UPSERT_SOURCE_FULL_DELETES,
                        SinkTable.UPSERT_SINK),
                TestSpec.join(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES,
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES,
                        SinkTable.UPSERT_SINK_FULL_DELETES),
                TestSpec.join(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES,
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES,
                        SinkTable.RETRACT_SINK),
                TestSpec.select(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES_METADATA,
                        SinkTable.UPSERT_SINK_METADATA),
                TestSpec.selectWithoutMetadata(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES_METADATA, SinkTable.UPSERT_SINK),
                TestSpec.select(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES_METADATA_NO_PUSHDOWN,
                        SinkTable.UPSERT_SINK_METADATA),
                TestSpec.selectWithoutMetadata(
                        SourceTable.UPSERT_SOURCE_PARTIAL_DELETES_METADATA_NO_PUSHDOWN,
                        SinkTable.UPSERT_SINK));
    }

    @AfterEach
    void tearDown() {
        Arrays.stream(util.tableEnv().listTables())
                .forEach(t -> util.tableEnv().executeSql("DROP TABLE " + t));
    }

    @ParameterizedTest()
    @MethodSource("getTests")
    void testChangelogNormalizePlan(TestSpec spec) {
        for (TableProperties tableProperties : spec.tablesToCreate) {
            final String additionalColumns =
                    String.join(",\n", tableProperties.getAdditionalColumns());
            util.tableEnv()
                    .executeSql(
                            String.format(
                                    "CREATE TABLE %s ( id INT,\n"
                                            + " col1 INT,\n"
                                            + " col2 STRING,\n"
                                            + "%s"
                                            + " PRIMARY KEY(id) NOT ENFORCED) WITH (%s)",
                                    tableProperties.getTableName(),
                                    StringUtils.isNullOrWhitespaceOnly(additionalColumns)
                                            ? ""
                                            : additionalColumns + ",\n",
                                    String.join(",\n", tableProperties.getOptions())));
        }
        util.verifyRelPlanInsert(
                spec.query,
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.CHANGELOG_MODE)));
    }

    interface TableProperties {

        String getTableName();

        List<String> getOptions();

        List<String> getAdditionalColumns();
    }

    public enum SourceTable implements TableProperties {
        UPSERT_SOURCE_PARTIAL_DELETES(
                "upsert_table_partial_deletes",
                "'changelog-mode' = 'UA,D'",
                "'source.produces-delete-by-key'='true'"),
        UPSERT_SOURCE_PARTIAL_DELETES_METADATA(
                "upsert_table_partial_deletes_metadata",
                List.of("`offset` BIGINT METADATA"),
                "'changelog-mode' = 'UA,D'",
                "'source.produces-delete-by-key'='true'",
                "'readable-metadata' = 'offset:BIGINT'"),
        UPSERT_SOURCE_PARTIAL_DELETES_METADATA_NO_PUSHDOWN(
                "upsert_table_partial_deletes_metadata_no_pushdown",
                List.of("`offset` BIGINT METADATA"),
                "'changelog-mode' = 'UA,D'",
                "'source.produces-delete-by-key'='true'",
                "'enable-projection-push-down'='false'",
                "'readable-metadata' = 'offset:BIGINT'"),
        UPSERT_SOURCE_FULL_DELETES(
                "upsert_table_full_deletes",
                "'changelog-mode' = 'UA,D'",
                "'source.produces-delete-by-key'='false'");

        private final String tableName;
        private final List<String> options;
        private final List<String> additionalColumns;

        SourceTable(String tableName, String... options) {
            this(tableName, Collections.emptyList(), options);
        }

        SourceTable(String tableName, List<String> additionalColumns, String... options) {
            this.tableName = tableName;
            this.additionalColumns = additionalColumns;
            this.options =
                    Stream.concat(
                                    Stream.of(options),
                                    Stream.of(
                                            "'connector' = 'values'",
                                            "'disable-lookup'='true'",
                                            "'runtime-source'='NewSource'"))
                            .collect(Collectors.toList());
        }

        @Override
        public String getTableName() {
            return tableName;
        }

        @Override
        public List<String> getOptions() {
            return options;
        }

        @Override
        public List<String> getAdditionalColumns() {
            return additionalColumns;
        }
    }

    public enum SinkTable implements TableProperties {
        UPSERT_SINK(
                "upsert_sink_table",
                "  'connector' = 'values'",
                "'sink.supports-delete-by-key' = 'true'",
                "'sink-changelog-mode-enforced' = 'I,UA,D'"),
        UPSERT_SINK_METADATA(
                "upsert_sink_table",
                List.of("`offset` BIGINT METADATA"),
                "  'connector' = 'values'",
                "'sink.supports-delete-by-key' = 'true'",
                "'writable-metadata' = 'offset:BIGINT'",
                "'sink-changelog-mode-enforced' = 'I,UA,D'"),
        UPSERT_SINK_FULL_DELETES(
                "upsert_sink_table_full_deletes",
                "  'connector' = 'values'",
                "'sink.supports-delete-by-key' = 'false'",
                "'sink-changelog-mode-enforced' = 'I,UA,D'"),
        RETRACT_SINK(
                "all_change_sink_table",
                "'connector' = 'values'",
                "'sink-changelog-mode-enforced' = 'I,UA,UB,D'");

        private final String tableName;
        private final List<String> options;
        private final List<String> additionalColumns;

        SinkTable(String tableName, String... options) {
            this(tableName, Collections.emptyList(), options);
        }

        SinkTable(String tableName, List<String> additionalColumns, String... options) {
            this.tableName = tableName;
            this.options = Arrays.asList(options);
            this.additionalColumns = additionalColumns;
        }

        @Override
        public String getTableName() {
            return tableName;
        }

        @Override
        public List<String> getOptions() {
            return options;
        }

        @Override
        public List<String> getAdditionalColumns() {
            return additionalColumns;
        }
    }

    private static class TestSpec {

        private final Set<TableProperties> tablesToCreate;
        private final String query;
        private final String description;

        private TestSpec(String description, Set<TableProperties> tablesToCreate, String query) {
            this.tablesToCreate = tablesToCreate;
            this.query = query;
            this.description = description;
        }

        public static TestSpec selectWithoutMetadata(SourceTable sourceTable, SinkTable sinkTable) {
            return new TestSpec(
                    String.format(
                            "select_no_metadata_%s_into_%s",
                            sourceTable.getTableName(), sinkTable.getTableName()),
                    new HashSet<>(Arrays.asList(sourceTable, sinkTable)),
                    String.format(
                            "INSERT INTO %s SELECT id, col1, col2 FROM %s",
                            sinkTable.getTableName(), sourceTable.getTableName()));
        }

        public static TestSpec select(SourceTable sourceTable, SinkTable sinkTable) {
            return new TestSpec(
                    String.format(
                            "select_%s_into_%s",
                            sourceTable.getTableName(), sinkTable.getTableName()),
                    new HashSet<>(Arrays.asList(sourceTable, sinkTable)),
                    String.format(
                            "INSERT INTO %s SELECT * FROM %s",
                            sinkTable.getTableName(), sourceTable.getTableName()));
        }

        public static TestSpec selectWithFilter(SourceTable sourceTable, SinkTable sinkTable) {
            return new TestSpec(
                    String.format(
                            "select_with_filter_%s_into_%s",
                            sourceTable.getTableName(), sinkTable.getTableName()),
                    new HashSet<>(Arrays.asList(sourceTable, sinkTable)),
                    String.format(
                            "INSERT INTO %s SELECT * FROM %s WHERE col1 > 2",
                            sinkTable.getTableName(), sourceTable.getTableName()));
        }

        public static TestSpec join(
                SourceTable leftTable, SourceTable rightTable, SinkTable sinkTable) {
            return new TestSpec(
                    String.format(
                            "join_%s_%s_into_%s",
                            leftTable.getTableName(),
                            rightTable.getTableName(),
                            sinkTable.getTableName()),
                    new HashSet<>(Arrays.asList(leftTable, rightTable, sinkTable)),
                    String.format(
                            "INSERT INTO %s SELECT l.* FROM %s l JOIN %s r ON l.id = r.id",
                            sinkTable.getTableName(),
                            leftTable.getTableName(),
                            rightTable.getTableName()));
        }

        @Override
        public String toString() {
            return description;
        }
    }
}

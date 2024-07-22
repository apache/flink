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

package org.apache.flink.table.gateway.service.materializedtable;

import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link MaterializedTableManager}. */
class MaterializedTableManagerTest {

    @Test
    void testGetManuallyRefreshStatement() {
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of("catalog", "database", "my_materialized_table");
        String query = "SELECT * FROM my_source_table";
        assertThat(
                        MaterializedTableManager.getRefreshStatement(
                                tableIdentifier,
                                query,
                                Collections.emptyMap(),
                                Collections.emptyMap()))
                .isEqualTo(
                        "INSERT OVERWRITE `catalog`.`database`.`my_materialized_table`\n"
                                + "  SELECT * FROM (SELECT * FROM my_source_table)");

        Map<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put("k1", "v1");
        partitionSpec.put("k2", "v2");
        assertThat(
                        MaterializedTableManager.getRefreshStatement(
                                tableIdentifier, query, partitionSpec, Collections.emptyMap()))
                .isEqualTo(
                        "INSERT OVERWRITE `catalog`.`database`.`my_materialized_table`\n"
                                + "  SELECT * FROM (SELECT * FROM my_source_table)\n"
                                + "  WHERE k1 = 'v1' AND k2 = 'v2'");
    }

    @Test
    void testGenerateInsertStatement() {
        // Test generate insert crate statement
        ObjectIdentifier materializedTableIdentifier =
                ObjectIdentifier.of("catalog", "database", "table");
        String definitionQuery = "SELECT * FROM source_table";
        String expectedStatement =
                "INSERT INTO `catalog`.`database`.`table`\n" + "SELECT * FROM source_table";

        String actualStatement =
                MaterializedTableManager.getInsertStatement(
                        materializedTableIdentifier, definitionQuery, Collections.emptyMap());

        assertThat(actualStatement).isEqualTo(expectedStatement);
    }

    @Test
    void testGenerateInsertStatementWithDynamicOptions() {
        ObjectIdentifier materializedTableIdentifier =
                ObjectIdentifier.of("catalog", "database", "table");
        String definitionQuery = "SELECT * FROM source_table";
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put("option1", "value1");
        dynamicOptions.put("option2", "value2");

        String expectedStatement =
                "INSERT INTO `catalog`.`database`.`table` "
                        + "/*+ OPTIONS('option1'='value1', 'option2'='value2') */\n"
                        + "SELECT * FROM source_table";

        String actualStatement =
                MaterializedTableManager.getInsertStatement(
                        materializedTableIdentifier, definitionQuery, dynamicOptions);
        assertThat(actualStatement).isEqualTo(expectedStatement);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testGetPeriodRefreshPartition(TestSpec testSpec) {
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of("catalog", "database", "table");

        if (testSpec.errorMessage == null) {
            Map<String, String> actualRefreshPartition =
                    MaterializedTableManager.getPeriodRefreshPartition(
                            testSpec.schedulerTime,
                            testSpec.freshness,
                            objectIdentifier,
                            testSpec.tableOptions,
                            ZoneId.systemDefault());

            assertThat(actualRefreshPartition).isEqualTo(testSpec.expectedRefreshPartition);
        } else {
            assertThatThrownBy(
                            () ->
                                    MaterializedTableManager.getPeriodRefreshPartition(
                                            testSpec.schedulerTime,
                                            testSpec.freshness,
                                            objectIdentifier,
                                            testSpec.tableOptions,
                                            ZoneId.systemDefault()))
                    .hasMessage(testSpec.errorMessage);
        }
    }

    static Stream<TestSpec> testData() {
        return Stream.of(
                // The interval of freshness match the partition specified by the 'date-formatter'.
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofDay("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-02 00:00:00")
                        .freshness(IntervalFreshness.ofDay("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-02 00:00:00")
                        .freshness(IntervalFreshness.ofHour("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-02 01:00:00")
                        .freshness(IntervalFreshness.ofHour("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2024-01-02")
                        .expectedRefreshPartition("hour", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour("2"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "22"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour("4"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "20"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour("8"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "16"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour("12"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "12"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 12:00:00")
                        .freshness(IntervalFreshness.ofHour("12"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "59"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("2"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "58"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("4"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "56"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("5"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "55"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("6"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "54"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("10"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "50"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("12"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "48"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("15"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "45"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("30"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "30"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:30:00")
                        .freshness(IntervalFreshness.ofMinute("30"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "00")
                        .expectedRefreshPartition("minute", "00"),

                // The interval of freshness is larger than the partition specified by the
                // 'date-formatter'.
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofDay("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofDay("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "00")
                        .expectedRefreshPartition("minute", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 01:00:00")
                        .freshness(IntervalFreshness.ofHour("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "00")
                        .expectedRefreshPartition("minute", "00"),
                // The interval of freshness is less than the partition specified by the
                // 'date-formatter'.
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 01:00:00")
                        .freshness(IntervalFreshness.ofHour("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour("2"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 02:00:00")
                        .freshness(IntervalFreshness.ofHour("2"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour("4"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 04:00:00")
                        .freshness(IntervalFreshness.ofHour("4"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("2"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("4"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("15"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:01:00")
                        .freshness(IntervalFreshness.ofMinute("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),

                // Invalid test case.
                TestSpec.create()
                        .schedulerTime(null)
                        .freshness(IntervalFreshness.ofDay("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .errorMessage(
                                "The scheduler time must not be null during the periodic refresh of the materialized table `catalog`.`database`.`table`."),
                TestSpec.create()
                        .schedulerTime("2024-01-01")
                        .freshness(IntervalFreshness.ofDay("1"))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .errorMessage(
                                "Failed to parse a valid partition value for the field 'day' in materialized table `catalog`.`database`.`table` using the scheduler time '2024-01-01' based on the date format 'yyyy-MM-dd HH:mm:ss'."));
    }

    private static class TestSpec {
        private String schedulerTime;
        private IntervalFreshness freshness;
        private final Map<String, String> tableOptions;
        private final Map<String, String> expectedRefreshPartition;

        private @Nullable String errorMessage;

        private TestSpec() {
            this.tableOptions = new HashMap<>();
            this.expectedRefreshPartition = new HashMap<>();
        }

        public static TestSpec create() {
            return new TestSpec();
        }

        public TestSpec schedulerTime(String schedulerTime) {
            this.schedulerTime = schedulerTime;
            return this;
        }

        public TestSpec freshness(IntervalFreshness freshness) {
            this.freshness = freshness;
            return this;
        }

        public TestSpec tableOptions(String key, String value) {
            this.tableOptions.put(key, value);
            return this;
        }

        public TestSpec expectedRefreshPartition(String key, String value) {
            this.expectedRefreshPartition.put(key, value);
            return this;
        }

        public TestSpec errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "schedulerTime="
                    + schedulerTime
                    + ", freshness="
                    + freshness
                    + ", tableOptions="
                    + tableOptions
                    + ", expectedRefreshPartition="
                    + expectedRefreshPartition
                    + '}';
        }
    }
}

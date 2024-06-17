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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

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

    @Test
    void testGetPeriodRefreshPartition() {
        String schedulerTime = "2024-01-01 00:00:00";
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("partition.fields.day.date-formatter", "yyyy-MM-dd");
        tableOptions.put("partition.fields.hour.date-formatter", "HH");

        ObjectIdentifier objectIdentifier = ObjectIdentifier.of("catalog", "database", "table");

        Map<String, String> actualRefreshPartition =
                MaterializedTableManager.getPeriodRefreshPartition(
                        schedulerTime, objectIdentifier, tableOptions, ZoneId.systemDefault());
        Map<String, String> expectedRefreshPartition = new HashMap<>();
        expectedRefreshPartition.put("day", "2024-01-01");
        expectedRefreshPartition.put("hour", "00");

        assertThat(actualRefreshPartition).isEqualTo(expectedRefreshPartition);
    }

    @Test
    void testGetPeriodRefreshPartitionWithInvalidSchedulerTime() {
        // scheduler time is null
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("partition.fields.day.date-formatter", "yyyy-MM-dd");
        tableOptions.put("partition.fields.hour.date-formatter", "HH");

        ObjectIdentifier objectIdentifier = ObjectIdentifier.of("catalog", "database", "table");

        assertThatThrownBy(
                        () ->
                                MaterializedTableManager.getPeriodRefreshPartition(
                                        null,
                                        objectIdentifier,
                                        tableOptions,
                                        ZoneId.systemDefault()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Scheduler time not properly set for periodic refresh of materialized table `catalog`.`database`.`table`.");

        // scheduler time is invalid
        String invalidSchedulerTime = "2024-01-01";
        assertThatThrownBy(
                        () ->
                                MaterializedTableManager.getPeriodRefreshPartition(
                                        invalidSchedulerTime,
                                        objectIdentifier,
                                        tableOptions,
                                        ZoneId.systemDefault()))
                .isInstanceOf(SqlExecutionException.class)
                .hasMessage(
                        "Failed to parse a valid partition value for the field 'day' in materialized table `catalog`.`database`.`table` using the scheduler time '2024-01-01' based on the date format 'yyyy-MM-dd HH:mm:ss'.");
    }
}

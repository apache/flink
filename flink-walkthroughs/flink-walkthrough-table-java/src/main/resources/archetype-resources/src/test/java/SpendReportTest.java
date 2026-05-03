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

package ${package};

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import org.apache.flink.util.CloseableIterator;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the SpendReport using batch mode.
 *
 * <p>One of Flink's unique properties is that it provides consistent semantics across
 * batch and streaming. This means you can develop and test applications in batch mode
 * on static datasets, and deploy to production as streaming applications.
 */
public class SpendReportTest {

    @Test
    public void testReport() throws Exception {
        // Use batch mode for testing with static data
        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create test data using fromValues
        Table transactions = tEnv.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("accountId", DataTypes.BIGINT()),
                DataTypes.FIELD("amount", DataTypes.BIGINT()),
                DataTypes.FIELD("transactionTime", DataTypes.TIMESTAMP(3))
            ),
            Row.of(1L, 188L, LocalDateTime.of(2024, 1, 1, 9, 0, 0)),
            Row.of(1L, 374L, LocalDateTime.of(2024, 1, 1, 9, 30, 0)),
            Row.of(2L, 122L, LocalDateTime.of(2024, 1, 1, 10, 0, 0)),
            Row.of(1L, 478L, LocalDateTime.of(2024, 1, 1, 10, 30, 0)),
            Row.of(3L, 64L, LocalDateTime.of(2024, 1, 1, 11, 0, 0))
        );

        // Apply the report function
        Table result = SpendReport.report(transactions);

        // Collect results
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.execute().collect()) {
            iterator.forEachRemaining(rows::add);
        }

        // Verify results
        // Account 1 at 9:00 should have 188 + 374 = 562
        // Account 1 at 10:00 should have 478
        // Account 2 at 10:00 should have 122
        // Account 3 at 11:00 should have 64
        assertEquals(4, rows.size(), "Expected 4 result rows");

        // Print results for visual verification
        System.out.println("Test Results:");
        for (Row row : rows) {
            System.out.println(row);
        }

        // Verify that all expected accounts are present
        boolean hasAccount1Hour9 = rows.stream()
            .anyMatch(r -> r.getField(0).equals(1L) &&
                          ((LocalDateTime) r.getField(1)).getHour() == 9 &&
                          r.getField(2).equals(562L));
        assertTrue(hasAccount1Hour9, "Expected account 1 at hour 9 with amount 562");
    }

    @Test
    public void testReportWithUDF() throws Exception {
        // Use batch mode for testing
        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create test data
        Table transactions = tEnv.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("accountId", DataTypes.BIGINT()),
                DataTypes.FIELD("amount", DataTypes.BIGINT()),
                DataTypes.FIELD("transactionTime", DataTypes.TIMESTAMP(3))
            ),
            Row.of(1L, 100L, LocalDateTime.of(2024, 1, 1, 9, 15, 0)),
            Row.of(1L, 200L, LocalDateTime.of(2024, 1, 1, 9, 45, 0))
        );

        // Test the MyFloor UDF directly
        MyFloor myFloor = new MyFloor();
        LocalDateTime floored = myFloor.eval(LocalDateTime.of(2024, 1, 1, 9, 47, 32));
        assertEquals(LocalDateTime.of(2024, 1, 1, 9, 0, 0), floored,
            "MyFloor should truncate to hour");

        // Apply report and verify
        Table result = SpendReport.report(transactions);
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.execute().collect()) {
            iterator.forEachRemaining(rows::add);
        }

        assertEquals(1, rows.size(), "Expected 1 result row");
        assertEquals(300L, rows.get(0).getField(2), "Expected sum of 300");
    }
}

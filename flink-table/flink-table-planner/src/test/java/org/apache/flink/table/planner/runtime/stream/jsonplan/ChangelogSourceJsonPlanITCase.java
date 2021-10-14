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

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Integration tests for operations on changelog source, including upsert source. */
public class ChangelogSourceJsonPlanITCase extends JsonPlanTestBase {

    @Test
    public void testChangelogSource() throws Exception {
        registerChangelogSource();
        createTestNonInsertOnlyValuesSinkTable(
                "user_sink",
                "user_id STRING PRIMARY KEY NOT ENFORCED",
                "user_name STRING",
                "email STRING",
                "balance DECIMAL(18,2)",
                "balance2 DECIMAL(18,2)");

        String dml = "INSERT INTO user_sink SELECT * FROM users";
        executeSqlWithJsonPlanVerified(dml).await();

        List<String> expected =
                Arrays.asList(
                        "+I[user1, Tom, tom123@gmail.com, 8.10, 16.20]",
                        "+I[user3, Bailey, bailey@qq.com, 9.99, 19.98]",
                        "+I[user4, Tina, tina@gmail.com, 11.30, 22.60]");
        assertResult(expected, TestValuesTableFactory.getResults("user_sink"));
    }

    @Test
    public void testToUpsertSource() throws Exception {
        registerUpsertSource();
        createTestNonInsertOnlyValuesSinkTable(
                "user_sink",
                "user_id STRING PRIMARY KEY NOT ENFORCED",
                "user_name STRING",
                "email STRING",
                "balance DECIMAL(18,2)",
                "balance2 DECIMAL(18,2)");

        String dml = "INSERT INTO user_sink SELECT * FROM users";
        executeSqlWithJsonPlanVerified(dml).await();

        List<String> expected =
                Arrays.asList(
                        "+I[user1, Tom, tom123@gmail.com, 8.10, 16.20]",
                        "+I[user3, Bailey, bailey@qq.com, 9.99, 19.98]",
                        "+I[user4, Tina, tina@gmail.com, 11.30, 22.60]");
        assertResult(expected, TestValuesTableFactory.getResults("user_sink"));
    }

    // ------------------------------------------------------------------------------------------

    public void registerChangelogSource() {
        Map<String, String> properties = new HashMap<>();
        properties.put("changelog-mode", "I,UA,UB,D");
        createTestValuesSourceTable(
                "users",
                JavaScalaConversionUtil.toJava(TestData.userChangelog()),
                new String[] {
                    "user_id STRING",
                    "user_name STRING",
                    "email STRING",
                    "balance DECIMAL(18,2)",
                    "balance2 AS balance * 2"
                },
                properties);
    }

    public void registerUpsertSource() {
        Map<String, String> properties = new HashMap<>();
        properties.put("changelog-mode", "I,UA,D");
        createTestValuesSourceTable(
                "users",
                JavaScalaConversionUtil.toJava(TestData.userUpsertlog()),
                new String[] {
                    "user_id STRING PRIMARY KEY NOT ENFORCED",
                    "user_name STRING",
                    "email STRING",
                    "balance DECIMAL(18,2)",
                    "balance2 AS balance * 2"
                },
                properties);
    }
}

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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.table.planner.delegation.hive.HiveParserConstants;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Test the compatibility with SQL11 reserved keywords in hive queries. */
class HiveDialectSupportSQL11ReservedKeywordAsIdentifierTest {
    private static HiveCatalog hiveCatalog;
    private static TableEnvironment tableEnv;
    private static List<String> sql11ReservedKeywords;

    @BeforeAll
    static void setup() throws Exception {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog
                .getHiveConf()
                .setBoolean(
                        HiveParserConstants
                                .SUPPORT_SQL11_RESERVED_KEYWORDS_AS_IDENTIFIERS_CONFIG_NAME,
                        false);
        hiveCatalog.open();
        tableEnv = getTableEnvWithHiveCatalog();
        tableEnv.getConfig().set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);
        sql11ReservedKeywords =
                new ArrayList<>(
                        Arrays.asList(
                                "ALL",
                                "ALTER",
                                "ARRAY",
                                "AS",
                                "AUTHORIZATION",
                                "BETWEEN",
                                "BIGINT",
                                "BINARY",
                                "BOOLEAN",
                                "BOTH",
                                "BY",
                                "CREATE",
                                "CUBE",
                                "CURRENT_DATE",
                                "CURRENT_TIMESTAMP",
                                "CURSOR",
                                "DATE",
                                "DECIMAL",
                                "DELETE",
                                "DESCRIBE",
                                "DOUBLE",
                                "DROP",
                                "EXISTS",
                                "EXTERNAL",
                                "FALSE",
                                "FETCH",
                                "FLOAT",
                                "FOR",
                                "FULL",
                                "GRANT",
                                "GROUP",
                                "GROUPING",
                                "IMPORT",
                                "IN",
                                "INNER",
                                "INSERT",
                                "INT",
                                "INTERSECT",
                                "INTO",
                                "IS",
                                "LATERAL",
                                "LEFT",
                                "LIKE",
                                "LOCAL",
                                "NONE",
                                "NULL",
                                "OF",
                                "ORDER",
                                "OUT",
                                "OUTER",
                                "PARTITION",
                                "PERCENT",
                                "PROCEDURE",
                                "RANGE",
                                "READS",
                                "REVOKE",
                                "RIGHT",
                                "ROLLUP",
                                "ROW",
                                "ROWS",
                                "SET",
                                "SMALLINT",
                                "TABLE",
                                "TIMESTAMP",
                                "TO",
                                "TRIGGER",
                                "TRUE",
                                "TRUNCATE",
                                "UNION",
                                "UPDATE",
                                "USER",
                                "USING",
                                "VALUES",
                                "WITH",
                                "REGEXP",
                                "RLIKE",
                                "PRIMARY",
                                "FOREIGN",
                                "CONSTRAINT",
                                "REFERENCES",
                                "PRECISION"));
    }

    @Test
    void testReservedKeywordAsIdentifierInDDL() {
        List<String> toRun =
                new ArrayList<>(
                        Arrays.asList(
                                "create table table1 (x int, %s int)",
                                "create table table2 (x int) partitioned by (%s string, q string)",
                                "create table table3 (\n"
                                        + "  a int,\n"
                                        + "  %s struct<f1: boolean, f2: string, f3: struct<f4: int, f5: double>, f6: int>\n"
                                        + ")"));
        Random random = new Random();
        for (String queryTemplate : toRun) {
            // Select a random keyword.
            String chosenKeyword =
                    sql11ReservedKeywords.get(random.nextInt(sql11ReservedKeywords.size()));
            String finalQuery = String.format(queryTemplate, chosenKeyword);
            runQuery(finalQuery);
        }
    }

    @Test
    void testReservedKeywordAsIdentifierInDQL() {
        List<String> toRun =
                new ArrayList<>(
                        Arrays.asList(
                                "create table table4(id int,name string,dep string,%s int,age int)",
                                "select avg(%s) over (partition by dep) as avgsal from table4",
                                "select dep,name,%s from (select dep,name,%s,rank() over "
                                        + "(partition by dep order by %s desc) as rnk from table4) a where rnk=1",
                                "select %s,sum(cnt) over (order by %s)/sum(cnt) over "
                                        + "(order by %s ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from"
                                        + " (select %s,count(*) as cnt from table4 group by %s) a"));
        Random random = new Random();
        // Select a random keyword.
        String chosenKeyword =
                sql11ReservedKeywords.get(random.nextInt(sql11ReservedKeywords.size()));
        for (String queryTemplate : toRun) {
            String finalQuery = queryTemplate.replace("%s", chosenKeyword);
            runQuery(finalQuery);
        }
    }

    @Test
    void testReservedKeywordAsIdentifierInDML() {
        List<String> toRun =
                new ArrayList<>(
                        Arrays.asList(
                                "create table table5 (%s string, value string)",
                                "create table table6(key int, ten int, one int, value string)",
                                "from table5 insert overwrite table table6 map table5.%s,"
                                        + " CAST(table5.%s / 10 AS INT), CAST(table5.%s % 10 AS INT),"
                                        + " table5.value using 'cat' as (tkey, ten, one, tvalue)"
                                        + " distribute by tvalue, tkey"));
        Random random = new Random();
        // Select a random keyword.
        String chosenKeyword =
                sql11ReservedKeywords.get(random.nextInt(sql11ReservedKeywords.size()));
        for (String queryTemplate : toRun) {
            String finalQuery = queryTemplate.replace("%s", chosenKeyword);
            runQuery(finalQuery);
        }
    }

    private void runQuery(String query) {
        try {
            CollectionUtil.iteratorToList(tableEnv.executeSql(query).collect());
        } catch (Exception e) {
            System.out.println("Failed to run " + query);
            throw e;
        }
    }

    private static TableEnvironment getTableEnvWithHiveCatalog() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        // automatically load hive module in hive-compatible mode
        HiveModule hiveModule = new HiveModule(hiveCatalog.getHiveVersion());
        CoreModule coreModule = CoreModule.INSTANCE;
        for (String loaded : tableEnv.listModules()) {
            tableEnv.unloadModule(loaded);
        }
        tableEnv.loadModule("hive", hiveModule);
        tableEnv.loadModule("core", coreModule);
        return tableEnv;
    }
}

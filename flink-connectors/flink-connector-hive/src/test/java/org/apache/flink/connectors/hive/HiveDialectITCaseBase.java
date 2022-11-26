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

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;

/** Base class for hive query compatibility test. */
public abstract class HiveDialectITCaseBase {

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    protected static final String QTEST_DIR =
            Thread.currentThread().getContextClassLoader().getResource("query-test").getPath();
    protected static final String SORT_QUERY_RESULTS = "SORT_QUERY_RESULTS";

    protected static HiveCatalog hiveCatalog;
    protected static TableEnvironment tableEnv;
    protected static String warehouse;

    @BeforeClass
    public static void setup() throws Exception {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        // required by query like "src.`[k].*` from src"
        hiveCatalog.getHiveConf().setVar(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT, "none");
        hiveCatalog.open();
        tableEnv = getTableEnvWithHiveCatalog();
        warehouse = hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);

        // create tables
        tableEnv.executeSql("create table foo (x int, y int)");
        tableEnv.executeSql("create table bar(I int, s string)");
        tableEnv.executeSql("create table baz(ai array<int>, d double)");
        tableEnv.executeSql(
                "create table employee(id int,name string,dep string,salary int,age int)");
        tableEnv.executeSql("create table dest (x int, y int)");
        tableEnv.executeSql("create table destp (x int) partitioned by (p string, q string)");
        tableEnv.executeSql("alter table destp add partition (p='-1',q='-1')");
        tableEnv.executeSql("CREATE TABLE src (key STRING, value STRING)");
        tableEnv.executeSql("CREATE TABLE t_sub_query (x int)");
        tableEnv.executeSql(
                "CREATE TABLE srcpart (key STRING, `value` STRING) PARTITIONED BY (ds STRING, hr STRING)");
        tableEnv.executeSql("create table binary_t (a int, ab array<binary>)");

        tableEnv.executeSql(
                "CREATE TABLE nested (\n"
                        + "  a int,\n"
                        + "  s1 struct<f1: boolean, f2: string, f3: struct<f4: int, f5: double>, f6: int>,\n"
                        + "  s2 struct<f7: string, f8: struct<f9 : boolean, f10: array<int>, f11: map<string, boolean>>>,\n"
                        + "  s3 struct<f12: array<struct<f13:string, f14:int>>>,\n"
                        + "  s4 map<string, struct<f15:int>>,\n"
                        + "  s5 struct<f16: array<struct<f17:string, f18:struct<f19:int>>>>,\n"
                        + "  s6 map<string, struct<f20:array<struct<f21:struct<f22:int>>>>>\n"
                        + ")");
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "foo")
                .addRow(new Object[] {1, 1})
                .addRow(new Object[] {2, 2})
                .addRow(new Object[] {3, 3})
                .addRow(new Object[] {4, 4})
                .addRow(new Object[] {5, 5})
                .commit();
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "bar")
                .addRow(new Object[] {1, "a"})
                .addRow(new Object[] {1, "aa"})
                .addRow(new Object[] {2, "b"})
                .commit();
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "baz")
                .addRow(new Object[] {Arrays.asList(1, 2, 3), 3.0})
                .commit();
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "src")
                .addRow(new Object[] {"1", "val1"})
                .addRow(new Object[] {"2", "val2"})
                .addRow(new Object[] {"3", "val3"})
                .commit();
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "t_sub_query")
                .addRow(new Object[] {2})
                .addRow(new Object[] {3})
                .commit();
        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "employee")
                .addRow(new Object[] {1, "A", "Management", 4500, 55})
                .addRow(new Object[] {2, "B", "Management", 4400, 61})
                .addRow(new Object[] {3, "C", "Management", 4000, 42})
                .addRow(new Object[] {4, "D", "Production", 3700, 35})
                .addRow(new Object[] {5, "E", "Production", 3500, 24})
                .addRow(new Object[] {6, "F", "Production", 3600, 28})
                .addRow(new Object[] {7, "G", "Production", 3800, 35})
                .addRow(new Object[] {8, "H", "Production", 4000, 52})
                .addRow(new Object[] {9, "I", "Service", 4100, 40})
                .addRow(new Object[] {10, "J", "Sales", 4300, 36})
                .addRow(new Object[] {11, "K", "Sales", 4100, 38})
                .commit();

        // create functions
        tableEnv.executeSql(
                "create function hiveudf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs'");
        tableEnv.executeSql(
                "create function hiveudtf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode'");
        tableEnv.executeSql(
                "create function myudtf as '"
                        + HiveDialectQueryITCase.MyUDTF.class.getName()
                        + "'");

        // create temp functions
        tableEnv.executeSql(
                "create temporary function temp_abs as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs'");
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

    protected void runQuery(String query) throws Exception {
        try {
            CollectionUtil.iteratorToList(tableEnv.executeSql(query).collect());
        } catch (Exception e) {
            System.out.println("Failed to run " + query);
            throw e;
        }
    }

    protected String explainSql(String sql) {
        return (String)
                CollectionUtil.iteratorToList(tableEnv.executeSql("explain " + sql).collect())
                        .get(0)
                        .getField(0);
    }
}

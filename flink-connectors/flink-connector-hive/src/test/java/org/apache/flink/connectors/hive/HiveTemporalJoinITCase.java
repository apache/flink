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

import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTest;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.filesystem.FileSystemOptions;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.types.Row;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Test Temporal join of hive tables.
 *
 * <p>Defining primary key only supports since hive 3.0.0, skip other versions only test in hive
 * 3.1.1. To run this test, please use mvn command: mvn test -Phive-3.1.1
 * -Dtest=org.apache.flink.connectors.hive.HiveTemporalJoinITCase
 */
public class HiveTemporalJoinITCase extends TableTestBase {

    private static TableEnvironment tableEnv;
    private static HiveCatalog hiveCatalog;

    @BeforeClass
    public static void setup() {
        if (!HiveVersionTestUtil.HIVE_310_OR_LATER) {
            return;
        }
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        tableEnv = TableEnvironment.create(settings);
        hiveCatalog = HiveTestUtils.createHiveCatalog();

        hiveCatalog = HiveTestUtils.createHiveCatalog(CatalogTest.TEST_CATALOG_NAME, "3.1.2");

        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        // create probe table
        TestCollectionTableFactory.initData(
                Arrays.asList(
                        Row.of(1, "a", Timestamp.valueOf("1970-01-01 00:00:00.001")),
                        Row.of(1, "c", Timestamp.valueOf("1970-01-01 00:00:00.002")),
                        Row.of(2, "b", Timestamp.valueOf("1970-01-01 00:00:00.003")),
                        Row.of(2, "c", Timestamp.valueOf("1970-01-01 00:00:00.004")),
                        Row.of(3, "c", Timestamp.valueOf("1970-01-01 00:00:00.005")),
                        Row.of(4, "d", Timestamp.valueOf("1970-01-01 00:00:00.006"))));
        tableEnv.executeSql(
                "create table default_catalog.default_database.probe ("
                        + " x int,"
                        + " y string,"
                        + " rowtime timestamp(3),"
                        + " p as proctime(),"
                        + " watermark for rowtime as rowtime) "
                        + "with ('connector'='COLLECTION','is-bounded' = 'false')");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql(
                String.format(
                        "create table build ("
                                + " x int, "
                                + " y string, "
                                + " z int, "
                                + " primary key(x,y) disable novalidate rely)"
                                + " tblproperties ('%s' = 'true', '%s'='5min')",
                        FileSystemOptions.STREAMING_SOURCE_ENABLE.key(),
                        FileSystemOptions.STREAMING_SOURCE_MONITOR_INTERVAL.key()));

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    }

    @Test
    public void testProcTimeTemporalJoinHiveTable() throws Exception {
        if (!HiveVersionTestUtil.HIVE_310_OR_LATER) {
            return;
        }
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("insert into build values (1,'a',10),(2,'a',21),(2,'b',22),(3,'c',33)")
                .await();

        expectedException().expect(TableException.class);
        expectedException().expectMessage("Processing-time temporal join is not supported yet.");
        tableEnv.executeSql(
                "select p.x, p.y, b.z from "
                        + " default_catalog.default_database.probe as p "
                        + " join build for system_time as of p.p as b on p.x=b.x and p.y=b.y");
    }

    @Test
    public void testRowTimeTemporalJoinHiveTable() throws Exception {
        if (!HiveVersionTestUtil.HIVE_310_OR_LATER) {
            return;
        }
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("insert into build values (1,'a',10),(2,'a',21),(2,'b',22),(3,'c',33)")
                .await();

        // Streaming hive table does not support defines watermark
        expectedException().expect(ValidationException.class);
        expectedException()
                .expectMessage(
                        "Event-Time Temporal Table Join requires both primary key"
                                + " and row time attribute in versioned table, but no row time attribute can be found.");
        tableEnv.executeSql(
                "select p.x, p.y, b.z from "
                        + " default_catalog.default_database.probe as p "
                        + " join build for system_time as of p.rowtime as b on p.x=b.x and p.y=b.y");
    }

    @AfterClass
    public static void tearDown() {
        if (!HiveVersionTestUtil.HIVE_310_OR_LATER) {
            return;
        }
        tableEnv.executeSql("drop table build");

        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }
}

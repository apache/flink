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

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.factories.TestTimeTravelCatalog;
import org.apache.flink.table.planner.runtime.utils.TimeTravelTestUtil;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.DateTimeTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Plan tests for time travel. */
public class TimeTravelTest extends TableTestBase {

    private BatchTableTestUtil util;

    @Before
    public void before() {
        util = batchTestUtil(TableConfig.getDefault());
        String catalogName = "TimeTravelCatalog";
        TestTimeTravelCatalog catalog =
                TimeTravelTestUtil.getTestingCatalogWithVersionedTable(catalogName, "t1");
        TableEnvironment tEnv = util.tableEnv();
        tEnv.registerCatalog(catalogName, catalog);
        tEnv.useCatalog(catalogName);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    public void testTimeTravel() {
        util.verifyExecPlan(
                "SELECT * FROM t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00'");
    }

    @Test
    public void testTimeTravelWithAsExpression() {
        util.verifyExecPlan(
                "SELECT * FROM t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00' AS t2");
    }

    @Test
    public void testTimeTravelWithSimpleExpression() {
        util.verifyExecPlan(
                "SELECT * FROM t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 00:00:00'+INTERVAL '60' DAY");
    }

    @Test
    public void testTimeTravelWithDifferentTimezone() {
        util.tableEnv().getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        util.verifyExecPlan(
                String.format(
                        "SELECT * FROM t1 FOR SYSTEM_TIME AS OF TIMESTAMP '%s'",
                        DateTimeTestUtil.timezoneConvert(
                                "2023-01-01 02:00:00",
                                "yyyy-MM-dd HH:mm:ss",
                                ZoneId.of("UTC"),
                                ZoneId.of("Asia/Shanghai"))));
    }

    @Test
    public void testTimeTravelOneTableMultiTimes() {
        util.verifyExecPlan(
                "SELECT\n"
                        + "    f1\n"
                        + "FROM\n"
                        + "    t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 01:00:00'\n"
                        + "UNION ALL\n"
                        + "SELECT\n"
                        + "    f2\n"
                        + "FROM\n"
                        + "    t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00'");
    }

    @Test
    public void testTimeTravelWithLookupJoin() {
        util.verifyExecPlan(
                "SELECT\n"
                        + "    l.f2,\n"
                        + "    r.f3\n"
                        + "FROM\n"
                        + "    (\n"
                        + "        SELECT\n"
                        + "            *,\n"
                        + "            proctime () as p\n"
                        + "        FROM\n"
                        + "            t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00'\n"
                        + "    ) l\n"
                        + "    LEFT JOIN t1 FOR SYSTEM_TIME AS OF l.p r ON l.f1=r.f1");
    }

    @Test
    public void testTimeTravelWithHints() {
        util.verifyExecPlan(
                "SELECT * FROM t1 /*+ options('bounded'='true') */ FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00' AS t2");
    }

    @Test
    public void testTimeTravelWithUnsupportedExpression() {
        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .executeSql(
                                                "SELECT\n"
                                                        + "    *\n"
                                                        + "FROM\n"
                                                        + "    t1 FOR SYSTEM_TIME AS OF TO_TIMESTAMP_LTZ (0, 3)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported time travel expression: TO_TIMESTAMP_LTZ(0, 3) for the expression can not be reduced to a constant by Flink.");

        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .executeSql(
                                                "SELECT\n"
                                                        + "    *\n"
                                                        + "FROM\n"
                                                        + "    t1 FOR SYSTEM_TIME AS OF PROCTIME()"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported time travel expression: PROCTIME() for the expression can not be reduced to a constant by Flink.");
    }

    @Test
    public void testTimeTravelWithIdentifierSnapshot() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE\n"
                                + "    t2 (f1 VARCHAR, f2 TIMESTAMP(3))\n"
                                + "WITH\n"
                                + "    ('connector'='values', 'bounded'='true')");

        // select snapshot with identifier only support in lookup join or temporal join.
        // The following query can't generate a validate execution plan.

        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .executeSql(
                                                "SELECT\n"
                                                        + "    *\n"
                                                        + "FROM\n"
                                                        + "    t2 FOR SYSTEM_TIME AS OF f2"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Cannot generate a valid execution plan for the given query");
    }

    @Test
    public void testTimeTravelWithView() {
        util.tableEnv().executeSql("CREATE VIEW tb_view AS SELECT * FROM t1");

        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .executeSql(
                                                "SELECT\n"
                                                        + "    *\n"
                                                        + "FROM\n"
                                                        + "    tb_view FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 01:00:00'"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "TimeTravelCatalog.default.tb_view is a view, but time travel is not supported for view.");
    }
}

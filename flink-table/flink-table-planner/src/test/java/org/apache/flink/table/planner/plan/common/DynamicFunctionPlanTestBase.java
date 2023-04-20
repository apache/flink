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

package org.apache.flink.table.planner.plan.common;

import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;

/** Plan test for queries contain dynamic functions. */
public abstract class DynamicFunctionPlanTestBase extends TableTestBase {

    private TableTestUtil util;

    protected abstract boolean isBatchMode();

    protected abstract TableTestUtil getTableTestUtil();

    @Before
    public void setup() {
        util = getTableTestUtil();

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE src (\n"
                                + " a INTEGER,\n"
                                + " b VARCHAR,\n"
                                + " cat VARCHAR,\n"
                                + " gmt_date DATE,\n"
                                + " cnt BIGINT,\n"
                                + " ts TIME,\n"
                                + " PRIMARY KEY (cat) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'\n"
                                + " ,'bounded' = '"
                                + isBatchMode()
                                + "'\n"
                                + ")");
    }

    @Test
    public void testAggregateReduceConstants() {
        util.verifyExecPlan(
                "SELECT\n"
                        + "     cat, gmt_date, SUM(cnt), count(*)\n"
                        + "FROM src\n"
                        + "WHERE gmt_date = current_date\n"
                        + "GROUP BY cat, gmt_date");
    }

    @Test
    public void testAggregateReduceConstants2() {
        // current RelMdPredicates only look at columns that are projected without any function
        // applied, so 'SUBSTR(CAST(LOCALTIME AS VARCHAR), 1, 2)' will never be inferred as constant
        util.verifyExecPlan(
                "SELECT\n"
                        + "cat, hh, SUM(cnt), COUNT(*)\n"
                        + "FROM (SELECT *, SUBSTR(CAST(LOCALTIME AS VARCHAR), 1, 2) hh FROM src)\n"
                        + "WHERE SUBSTR(CAST(ts AS VARCHAR), 1, 2) = hh\n"
                        + "GROUP BY cat, hh");
    }

    @Test
    public void testAggregateReduceConstants3() {
        util.verifyExecPlan(
                "SELECT\n"
                        + "     gmt_date, ts, cat, SUBSTR(CAST(ts AS VARCHAR), 1, 2), SUM(cnt)\n"
                        + "FROM src\n"
                        + "WHERE gmt_date = CURRENT_DATE\n"
                        + "  AND cat = 'fruit' AND ts = CURRENT_TIME\n"
                        + "GROUP BY gmt_date, ts, cat");
    }

    @Test
    public void testCalcMerge() {
        util.verifyExecPlan(
                "SELECT * FROM ( \n"
                        + "   SELECT *, SUBSTR(CAST(LOCALTIME AS VARCHAR), 1, 2) hh\n"
                        + "   FROM src\n"
                        + " ) t1 WHERE hh > 12 AND cat LIKE 'fruit%'\n");
    }
}

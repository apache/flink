/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.physical.common;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/** Test for StreamLocalAggregateUnionTransposeRule.LOCAL_WINDOW_AGG_INSTANCE. */
@RunWith(Parameterized.class)
public class LocalWindowAggregateUnionTransposeRuleTest extends TableTestBase {

    private TableTestUtil util;
    private TableConfig tableConfig;
    private final String unionSource =
            "CREATE VIEW UNION_SOURCE\n"
                    + "AS\n"
                    + "(SELECT\n"
                    + "  *\n"
                    + "FROM MyTable1\n"
                    + "UNION ALL\n"
                    + "SELECT\n"
                    + "  *\n"
                    + "FROM MyTable2\n"
                    + "UNION ALL\n"
                    + "SELECT\n"
                    + "  *\n"
                    + "FROM MyTable3)";
    private final String aggWithGroup =
            "SELECT\n"
                    + "   a,\n"
                    + "   window_start,\n"
                    + "   window_end,\n"
                    + "   count(*),\n"
                    + "   sum(d)\n"
                    + "FROM TABLE(TUMBLE(TABLE UNION_SOURCE, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))\n"
                    + "GROUP BY a, window_start, window_end";
    @Parameterized.Parameter public boolean isBatch;

    @Parameterized.Parameters(name = "isBatch: {0}")
    public static Collection<Boolean> isBatch() {
        return Arrays.asList(false, true);
    }

    @Before
    public void setup() {
        tableConfig = new TableConfig();
        if (isBatch) {
            util = batchTestUtil(tableConfig);
        } else {
            util = streamTestUtil(tableConfig);
        }
        String sourceTableTemplate =
                "CREATE TABLE %s (\n"
                        + "  a INT,\n"
                        + "  b BIGINT,\n"
                        + "  c STRING NOT NULL,\n"
                        + "  d DECIMAL(10, 3),\n"
                        + "  e BIGINT,\n"
                        + "  rowtime TIMESTAMP(3),\n"
                        + "  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = '%s'\n"
                        + ")";

        String ddl = String.format(sourceTableTemplate, "MyTable1", isBatch);
        util.tableEnv().executeSql(ddl);

        ddl = String.format(sourceTableTemplate, "MyTable2", isBatch);
        util.tableEnv().executeSql(ddl);

        ddl = String.format(sourceTableTemplate, "MyTable3", isBatch);
        util.tableEnv().executeSql(ddl);
    }

    @Test
    public void testLocalWindowAggregate() {
        // TODO support local window aggregate transpose in batch
        if (isBatch) {
            return;
        }
        util.tableEnv().executeSql(unionSource);
        util.verifyRelPlan(aggWithGroup);
    }
}

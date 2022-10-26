/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.sql.SqlMatchRecognize;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link SqlMatchRecognize}. */
public class MatchRecognizeTest extends TableTestBase {

    private BatchTableTestUtil util;

    @Before
    public void before() {
        util = batchTestUtil(TableConfig.getDefault());
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE Ticker (\n"
                                + "  `symbol` VARCHAR,\n"
                                + "  `price` INT,\n"
                                + "  `tax` INT,\n"
                                + "  `ts_ltz` as PROCTIME()\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'true'\n"
                                + ")");
    }

    @After
    public void after() {
        util.getTableEnv().executeSql("DROP TABLE Ticker");
    }

    @Test
    public void testCascadeMatch() {
        String sqlQuery =
                "SELECT *\n"
                        + "FROM (\n"
                        + "  SELECT\n"
                        + "    symbol,\n"
                        + "    price\n"
                        + "  FROM Ticker\n"
                        + "  MATCH_RECOGNIZE (\n"
                        + "    PARTITION BY symbol\n"
                        + "     ORDER BY ts_ltz"
                        + "    MEASURES\n"
                        + "      A.price as price,\n"
                        + "      A.tax as tax\n"
                        + "    ONE ROW PER MATCH\n"
                        + "    PATTERN (A)\n"
                        + "    DEFINE\n"
                        + "      A AS A.price > 0\n"
                        + "  ) AS T\n"
                        + "  GROUP BY symbol, price\n"
                        + ")\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  PARTITION BY symbol\n"
                        + "  MEASURES\n"
                        + "    A.price as dPrice\n"
                        + "  PATTERN (A)\n"
                        + "  DEFINE\n"
                        + "    A AS A.symbol = 'a'\n"
                        + ")";
        util.verifyExecPlan(sqlQuery);
    }
}

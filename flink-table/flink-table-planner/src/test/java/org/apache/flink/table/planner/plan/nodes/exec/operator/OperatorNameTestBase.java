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

package org.apache.flink.table.planner.plan.nodes.exec.operator;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableFunc1;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Base class for verifying name and description of SQL operators. */
@RunWith(Parameterized.class)
public abstract class OperatorNameTestBase extends TableTestBase {
    @Parameterized.Parameter public boolean isNameSimplifyEnabled;
    protected TableTestUtil util;
    protected TableEnvironment tEnv;

    @Parameterized.Parameters(name = "isNameSimplifyEnabled={0}")
    public static List<Boolean> testData() {
        return Arrays.asList(true, false);
    }

    @Before
    public void setup() {
        util = getTableTestUtil();
        util.getStreamEnv().setParallelism(2);
        tEnv = util.getTableEnv();
        tEnv.getConfig()
                .getConfiguration()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SIMPLIFY_OPERATOR_NAME_ENABLED,
                        isNameSimplifyEnabled);
    }

    protected void verifyQuery(String query) {
        util.verifyExplain(
                query,
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.JSON_EXECUTION_PLAN)));
    }

    protected void verifyInsert(String statement) {
        util.verifyExplainInsert(
                statement,
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.JSON_EXECUTION_PLAN)));
    }

    /** Verify Correlate and Calc. */
    @Test
    public void testCorrelate() {
        createTestSource();
        util.addTemporarySystemFunction("func1", new TableFunc1());
        verifyQuery("SELECT s FROM MyTable, LATERAL TABLE(func1(c)) AS T(s)");
    }

    /** Verify LookUpJoin. */
    @Test
    public void testLookupJoin() {
        createSourceWithTimeAttribute();
        String srcTableB =
                "CREATE TABLE LookupTable (\n"
                        + "  id int,\n"
                        + "  name varchar,\n"
                        + "  age int \n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true')";
        tEnv.executeSql(srcTableB);
        verifyQuery(
                "SELECT * FROM MyTable AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.b = D.id");
    }

    /** Verify GroupWindowAggregate. */
    @Test
    public void testGroupWindowAggregate() {
        createSourceWithTimeAttribute();
        verifyQuery(
                "SELECT\n"
                        + "  b,\n"
                        + "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE) as window_end,\n"
                        + "  COUNT(*)\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b, TUMBLE(rowtime, INTERVAL '15' MINUTE)");
    }

    /** Verify OverAggregate. */
    @Test
    public void testOverAggregate() {
        createSourceWithTimeAttribute();
        String sql =
                "SELECT b,\n"
                        + "    COUNT(a) OVER (PARTITION BY b ORDER BY rowtime\n"
                        + "        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt1\n"
                        + "FROM MyTable";
        verifyQuery(sql);
    }

    /** Verify Rank. */
    @Test
    public void testRank() {
        createTestSource();
        String sql =
                "SELECT a, row_num\n"
                        + "FROM (\n"
                        + "  SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) as row_num\n"
                        + "  FROM MyTable)\n"
                        + "WHERE row_num <= a";
        verifyQuery(sql);
    }

    /** Verify GroupAggregate. */
    protected void testGroupAggregateInternal() {
        createTestSource();
        verifyQuery("SELECT a, " + "max(b) as b " + "FROM MyTable GROUP BY a");
    }

    /** Verify Join. */
    protected void testJoinInternal() {
        createTestSource("A");
        createTestSource("B");
        verifyQuery("SELECT * from A, B where A.a = B.d");
    }

    protected void createTestSource() {
        createTestSource("MyTable");
    }

    protected void createTestSource(String tableName) {
        String srcTableDdl =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  a bigint,\n"
                                + "  b int not null,\n"
                                + "  c varchar,\n"
                                + "  d bigint not null\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'true')",
                        tableName);
        tEnv.executeSql(srcTableDdl);
    }

    protected void createSourceWithTimeAttribute() {
        createSourceWithTimeAttribute("MyTable");
    }

    protected void createSourceWithTimeAttribute(String name) {
        String srcTableDdl =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  a bigint,\n"
                                + "  b int not null,\n"
                                + "  c varchar,\n"
                                + "  d bigint not null,\n"
                                + "  rowtime timestamp(3),\n"
                                + "  proctime as proctime(),\n"
                                + "  watermark for rowtime AS rowtime - interval '1' second\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'true')",
                        name);
        tEnv.executeSql(srcTableDdl);
    }

    protected abstract TableTestUtil getTableTestUtil();
}

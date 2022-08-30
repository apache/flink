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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/** Test rule {@link ProjectSnapshotTransposeRule}. */
@RunWith(Parameterized.class)
public class ProjectSnapshotTransposeRuleTest extends TableTestBase {

    private static final String STREAM = "stream";
    private static final String BATCH = "batch";

    @Parameterized.Parameter public String mode;

    @Parameterized.Parameters(name = "mode = {0}")
    public static Collection<String> parameters() {
        return Arrays.asList(STREAM, BATCH);
    }

    private TableTestUtil util;

    @Before
    public void setup() {
        boolean isStreaming = STREAM.equals(mode);
        if (isStreaming) {
            util = streamTestUtil(TableConfig.getDefault());
            ((StreamTableTestUtil) util).buildStreamProgram(FlinkStreamProgram.LOGICAL_REWRITE());
        } else {
            util = batchTestUtil(TableConfig.getDefault());
            ((BatchTableTestUtil) util).buildBatchProgram(FlinkBatchProgram.LOGICAL_REWRITE());
        }

        TableEnvironment tEnv = util.getTableEnv();
        String src =
                String.format(
                        "CREATE TABLE MyTable (\n"
                                + "  a int,\n"
                                + "  b varchar,\n"
                                + "  c bigint,\n"
                                + "  proctime as PROCTIME(),\n"
                                + "  rowtime as TO_TIMESTAMP(FROM_UNIXTIME(c)),\n"
                                + "  watermark for rowtime as rowtime - INTERVAL '1' second \n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = '%s')",
                        !isStreaming);
        String lookup =
                String.format(
                        "CREATE TABLE LookupTable (\n"
                                + "  id int,\n"
                                + "  name varchar,\n"
                                + "  age int \n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = '%s')",
                        !isStreaming);
        tEnv.executeSql(src);
        tEnv.executeSql(lookup);
    }

    @Test
    public void testJoinTemporalTableWithProjectionPushDown() {
        String sql =
                "SELECT T.*, D.id\n"
                        + "FROM MyTable AS T\n"
                        + "JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D\n"
                        + "ON T.a = D.id";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinTemporalTableNotProjectable() {
        String sql =
                "SELECT T.*, D.*\n"
                        + "FROM MyTable AS T\n"
                        + "JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D\n"
                        + "ON T.a = D.id";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinTemporalTableWithReorderedProject() {
        String sql =
                "SELECT T.*, D.age, D.name, D.id\n"
                        + "FROM MyTable AS T\n"
                        + "JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D\n"
                        + "ON T.a = D.id";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinTemporalTableWithProjectAndFilter() {
        String sql =
                "SELECT T.*, D.id\n"
                        + "FROM MyTable AS T\n"
                        + "JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D\n"
                        + "ON T.a = D.id WHERE D.age > 20";

        util.verifyRelPlan(sql);
    }
}

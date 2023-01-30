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

package org.apache.flink.table.planner.analyze;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.expressions.utils.TestNonDeterministicUdf;
import org.apache.flink.table.planner.utils.PlanKind;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import scala.Enumeration;

import static scala.runtime.BoxedUnit.UNIT;

/** Test for {@link NonDeterministicUpdateAnalyzer}. */
public class NonDeterministicUpdateAnalyzerTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @Before
    public void before() {
        util.getTableEnv()
                .executeSql(
                        "create temporary table cdc (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  d bigint,\n"
                                + "  primary key (a) not enforced\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'changelog-mode' = 'I,UA,UB,D'\n"
                                + ")");
        util.getTableEnv()
                .executeSql(
                        "create temporary table cdc_with_meta (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  d boolean,\n"
                                + "  metadata_1 int metadata,\n"
                                + "  metadata_2 string metadata,\n"
                                + "  metadata_3 bigint metadata,\n"
                                + "  primary key (a) not enforced\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'changelog-mode' = 'I,UA,UB,D',\n"
                                + "  'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'\n"
                                + ")");
        util.getTableEnv()
                .executeSql(
                        "create temporary table sink_with_pk (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  primary key (a) not enforced\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false'\n"
                                + ")");
        util.getTableEnv()
                .executeSql(
                        "create temporary table sink_without_pk (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false'\n"
                                + ")");
        util.getTableEnv()
                .executeSql(
                        "create temporary table dim_with_pk (\n"
                                + " a int,\n"
                                + " b bigint,\n"
                                + " c string,\n"
                                + " primary key (a) not enforced\n"
                                + ") with (\n"
                                + " 'connector' = 'values'\n"
                                + ")");
        // custom ND function
        util.getTableEnv().createTemporaryFunction("ndFunc", new TestNonDeterministicUdf());
    }

    @Test
    public void testCdcWithMetaRenameSinkWithCompositePk() {
        // from NonDeterministicDagTest#testCdcWithMetaRenameSinkWithCompositePk
        util.getTableEnv()
                .executeSql(
                        "create temporary table cdc_with_meta_rename (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  d boolean,\n"
                                + "  metadata_3 bigint metadata,\n"
                                + "  e as metadata_3,\n"
                                + "  primary key (a) not enforced\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'changelog-mode' = 'I,UA,UB,D',\n"
                                + "  'readable-metadata' = 'metadata_3:BIGINT'\n"
                                + ")");
        util.getTableEnv()
                .executeSql(
                        "create temporary table sink_with_composite_pk (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  d bigint,\n"
                                + "  primary key (a,d) not enforced\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false'\n"
                                + ")");
        util.doVerifyPlanInsert(
                "insert into sink_with_composite_pk\n"
                        + "select a, b, c, e from cdc_with_meta_rename",
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()});
    }

    @Test
    public void testSourceWithComputedColumnSinkWithPk() {
        // from NonDeterministicDagTest#testSourceWithComputedColumnSinkWithPk
        util.getTableEnv()
                .executeSql(
                        "create temporary table cdc_with_computed_col (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  d int,\n"
                                + "  `day` as DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd'),\n"
                                + "  primary key(a, c) not enforced\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + " 'changelog-mode' = 'I,UA,UB,D'\n"
                                + ")");
        util.doVerifyPlanInsert(
                "insert into sink_with_pk\n"
                        + "select a, b, `day`\n"
                        + "from cdc_with_computed_col\n"
                        + "where b > 100",
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()});
    }

    @Test
    public void testCdcJoinDimWithPkNonDeterministicLocalCondition() {
        // from NonDeterministicDagTest#testCdcJoinDimWithPkNonDeterministicLocalCondition
        util.doVerifyPlanInsert(
                "insert into sink_without_pk\n"
                        + "select t1.a, t1.b, t1.c\n"
                        + "from (\n"
                        + "  select *, proctime() proctime from cdc\n"
                        + ") t1 join dim_with_pk for system_time as of t1.proctime as t2\n"
                        + "on t1.a = t2.a and ndFunc(t2.b) > 100",
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()});
    }

    @Test
    public void testCdcWithMetaSinkWithPk() {
        // from NonDeterministicDagTest#testCdcWithMetaSinkWithPk
        util.doVerifyPlanInsert(
                "insert into sink_with_pk\n" + "select a, metadata_3, c\n" + "from cdc_with_meta",
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()});
    }

    @Test
    public void testGroupByNonDeterministicFuncWithCdcSource() {
        // from NonDeterministicDagTest#testGroupByNonDeterministicFuncWithCdcSource
        util.doVerifyPlanInsert(
                "insert into sink_with_pk\n"
                        + "select\n"
                        + "  a, count(*) cnt, `day`\n"
                        + "from (\n"
                        + "  select *, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') `day` from cdc\n"
                        + ") t\n"
                        + "group by `day`, a",
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()});
    }

    @Test
    public void testMultiSinkOnJoinedView() {
        // from NonDeterministicDagTest#testMultiSinkOnJoinedView
        util.getTableEnv()
                .executeSql(
                        "create temporary table src1 (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  d int,\n"
                                + "  primary key(a, c) not enforced\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + " 'changelog-mode' = 'I,UA,UB,D'\n"
                                + ")");
        util.getTableEnv()
                .executeSql(
                        "create temporary table src2 (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  d int,\n"
                                + "  primary key(a, c) not enforced\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + " 'changelog-mode' = 'I,UA,UB,D'\n"
                                + ")");
        util.getTableEnv()
                .executeSql(
                        "create temporary table sink1 (\n"
                                + "  a int,\n"
                                + "  b string,\n"
                                + "  c bigint,\n"
                                + "  d bigint\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + " 'sink-insert-only' = 'false'\n"
                                + ")");
        util.getTableEnv()
                .executeSql(
                        "create temporary table sink2 (\n"
                                + "  a int,\n"
                                + "  b string,\n"
                                + "  c bigint,\n"
                                + "  d string\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + " 'sink-insert-only' = 'false'\n"
                                + ")");
        util.getTableEnv()
                .executeSql(
                        "create temporary view v1 as\n"
                                + "select\n"
                                + "  t1.a as a, t1.`day` as `day`, t2.b as b, t2.c as c\n"
                                + "from (\n"
                                + "  select a, b, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') as `day`\n"
                                + "  from src1\n"
                                + " ) t1\n"
                                + "join (\n"
                                + "  select b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `day`, c, d\n"
                                + "  from src2\n"
                                + ") t2\n"
                                + " on t1.a = t2.d");

        StatementSet stmtSet = util.getTableEnv().createStatementSet();
        stmtSet.addInsertSql(
                "insert into sink1\n"
                        + "  select a, `day`, sum(b), count(distinct c)\n"
                        + "  from v1\n"
                        + "  group by a, `day`");
        stmtSet.addInsertSql(
                "insert into sink2\n"
                        + "  select a, `day`, b, c\n"
                        + "  from v1\n"
                        + "  where b > 100");
        util.doVerifyPlan(
                stmtSet,
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()},
                () -> UNIT,
                false);
    }

    @Test
    public void testCdcJoinDimWithPkOutputNoPkSinkWithoutPk() {
        // from NonDeterministicDagTest#testCdcJoinDimWithPkOutputNoPkSinkWithoutPk
        util.doVerifyPlanInsert(
                "insert into sink_without_pk\n"
                        + "  select t1.a, t2.b, t1.c\n"
                        + "  from (\n"
                        + "    select *, proctime() proctime from cdc\n"
                        + "  ) t1 join dim_with_pk for system_time as of t1.proctime as t2\n"
                        + "  on t1.a = t2.a",
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()});
    }
}

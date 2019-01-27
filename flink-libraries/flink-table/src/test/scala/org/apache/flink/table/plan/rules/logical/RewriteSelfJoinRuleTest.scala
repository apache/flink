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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize.FlinkBatchPrograms._
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.rules.FlinkBatchExecRuleSets
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.util.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.junit.{Before, Ignore, Test}
import org.scalatest.prop.PropertyChecks

class RewriteSelfJoinRuleTest extends TableTestBase with PropertyChecks {
  private val util = nullableBatchTestUtil(false)
  private val tEnv = util.tableEnv

  @Before
  def before(): Unit = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()
    // convert queries before query decorrelation
    programs.addLast(
      SUBQUERY_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_REF_RULES)
          .build(), "convert table references before rewriting sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.SEMI_JOIN_RULES)
          .build(), "rewrite sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_SUBQUERY_RULES)
          .build(), "sub-queries remove")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_REF_RULES)
          .build(), "convert table references after sub-queries removed")
        .build()
    )

    // decorrelate
    programs.addLast(
      DECORRELATE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(new FlinkDecorrelateProgram, "decorrelate")
        .addProgram(new FlinkCorrelateVariablesValidationProgram, "correlate variables validation")
        .build()
    )

    programs.addLast(
      JOIN_REORDER,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchExecRuleSets.BATCH_EXEC_JOIN_REORDER)
        .build())
    // replace the programs.
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)
    util.addTable[(Int, Int, String, Int, String)]("person",
      Set(Set("name")), 'id, 'age, 'name, 'height, 'sex)

    util.addTable[(Int, Int, String, Int, String)]("t1",
      Set(Set("t1a")), 't1a, 't1b, 't1c, 't1d, 't1e)
    util.addTable[(Int, Int, String, Int, String)]("t2",
      Set(Set("t2a")), 't2a, 't2b, 't2c, 't2d, 't2e)
    // There would be a not null filter on t3a if we register it as PK.
    util.addTable[(Int, Int, String, Int, String)]("t3", 't3a, 't3b, 't3c, 't3d, 't3e)
    util.addTable[(Int, Int, String, Int, String)]("t4",
      Set(Set("t4a")), 't4a, 't4b, 't4c, 't4d, 't4e)
    util.addTable[(Int, Int, String, Int, String)]("t5",
      Set(Set("t5a")), 't5a, 't5b, 't5c, 't5d, 't5e)
  }

  @Test
  def testNestedMultiJoin(): Unit = {
    val sqlQuery =
      """
        |select t2e, t2c, t4c, t1a, t1c, t2d
        |from t1, t2, t3, t4, t5
        |where t1a = t3a
        |  and t2a = t3b
        |  and t1d = 19
        |  and t1e like '%bacc'
        |  and t2d = t4a
        |  and t4b = t5a
        |  and t5c = 'aabc'
        |  and t3d = (
        |      select min(t3d)
        |        from t2, t3, t4, t5
        |        where t1a = t3a
        |          and t2a = t3b
        |          and t2d = t4a
        |          and t4b = t5a
        |          and t5c = 'aabc'
        |)
      """.stripMargin
    verify(sqlQuery)
  }

  @Test @Ignore
  def testNestedMultiJoinWithMultiCorrelation(): Unit = {
    val sqlQuery =
      """
        |select t2e, t2c, t4c, t1a, t1c, t2d
        |from t1, t2, t3, t4, t5
        |where t1a = t3a
        |  and t1b = t2b
        |  and t2a = t3b
        |  and t1d = 19
        |  and t1e like '%bacc'
        |  and t2d = t4a
        |  and t4b = t5a
        |  and t5c = 'aabc'
        |  and t3d = (
        |      select min(t3d)
        |        from t2, t3, t4, t5
        |        where t1a = t3a
        |          and t1b = t2b
        |          and t2a = t3b
        |          and t2d = t4a
        |          and t4b = t5a
        |          and t5c = 'aabc'
        |)
      """.stripMargin
    verify(sqlQuery)
  }

  @Test
  def testWholeGroupWithSimpleView(): Unit = {
    val sqlQuery =
      """
        |select * from person where age = (select max(age) from person p)
      """.stripMargin
    verify(sqlQuery)
  }

  @Test
  def testWholeGroupWithAggView(): Unit = {
    val sqlQuery =
      """
        |select t2a, t2c, t2e, r.t3f
        |from t2,
        |  (select t3a as t3f, sum(t3b * (1 - t3d)) as t3g
        |    from t3
        |    where t3c >= date '1993-05-01'
        |      and t3c < date '1993-05-01' + interval '3' month
        |    group by
        |      t3a
        |  ) as r
        |where
        |  t2a = r.t3f
        |  and r.t3g = (
        |    select max(t3g)
        |    from
        |    (select t3a as t3f, sum(t3b * (1 - t3d)) as t3g
        |      from t3
        |      where t3c >= date '1993-05-01'
        |        and t3c < date '1993-05-01' + interval '3' month
        |      group by
        |      t3a)
        |  )
      """.stripMargin
    verify(sqlQuery)
  }

  @Test @Ignore
  def testWholeGroupWithNestedMultiJoinView(): Unit = {
    // Do not support it cause SegmentTopTransformRule will match outer/inner MultiJoin
    // separately.
    val sqlQuery =
      """
        |select t2e, t2c, t4c, t2d
        |from t2, t3, t4, t5
        |where
        |  t2a = t3b
        |  and t2d = t4a
        |  and t4b = t5a
        |  and t5c = 'aabc'
        |  and t3d = (
        |      select min(t3d)
        |        from t2, t3, t4, t5
        |        where
        |          t2a = t3b
        |          and t2d = t4a
        |          and t4b = t5a
        |          and t5c = 'aabc'
        |)
      """.stripMargin
    verify(sqlQuery)
  }

  @Test @Ignore
  def testNonPkCorrelation(): Unit = {
    // Do not support cause the PK filter is with `not null` after decorrelation.
    val sqlQuery =
      """
        |with
        |tmp as
        |(select max(age) as ma, height from person where sex = 'f' group by height)
        |SELECT person.*,tmp.ma from tmp, person
        |where person.age = tmp.ma and person.height = tmp.height
      """.stripMargin
    verify(sqlQuery)
  }

  @Test @Ignore
  def testPKCorrelation(): Unit = {
    // Do not support cause the PK filter is with `not null` after decorrelation.
    // Primary key on person.name.
    val sqlQuery =
      """
        |select * from person
        |where age = (select max(age)
        |             from person p
        |             where p.name = person.name)
      """.stripMargin
    verify(sqlQuery)
  }

  @Test @Ignore
  def testPKCorrelationWithNonAggFilter(): Unit = {
    // Do not support cause the PK filter is with `not null` after decorrelation.
    // Non agg side filter condition be more strict, correlate var is PK.
    val sqlQuery =
      """
        |select * from person where
        |age = (select max(age) from person p where p.name = person.name)
        |and height = 175
      """.stripMargin
    verify(sqlQuery)
  }

  @Test
  def testSameFiltersOnPKWithoutCorrelation(): Unit = {
    // The agg side、non-agg side filter condition be more strict, correlate var is not PK.
    val sqlQuery =
      """
        |select * from person
        |where age = (select max(age) from person
        |             where name = 'benji')
        |and name = 'benji'
      """.stripMargin
    verify(sqlQuery)
  }

  @Test
  def testNonPkCorrelationWithSameFilter(): Unit = {
    // The agg side filter condition be more strict, correlate var is not PK.
    val sqlQuery =
      """
        |select * from person
        |where age = (select max(age) from person p
        |             where name = 'benji' and p.height = person.height)
        |and name = 'benji'
      """.stripMargin
    verify(sqlQuery)
  }

  @Test
  def testNegative01(): Unit = {
    // Non agg side filter condition be more strict, correlate var is not PK.
    val sqlQuery =
      """
        |select * from person where
        |age = (select max(age) from person p where p.height = person.height)
        |and name = 'benji'
      """.stripMargin
    verify(sqlQuery)
  }

  @Test
  def testNegative02(): Unit = {
    // Agg side filter condition be more strict, correlate var is not PK.
    val sqlQuery =
      """
        |select * from person
        |where age = (select max(age)
        |             from person p
        |             where p.height = person.height and p.name = 'benji')
      """.stripMargin
    verify(sqlQuery)
  }

  def verify(query: String, onlyPrint: Boolean = false): Unit = {
    if (onlyPrint) {
      val resultTable = tEnv.sqlQuery(query)
      val relNode = tEnv.optimize(resultTable.getRelNode)
      println(FlinkRelOptUtil.toString(relNode))
    } else {
      util.verifyPlan(query)
    }
  }
}

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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase, TestFilterableTableSource}
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.FilterProjectTransposeRule
import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import org.junit.{Before, Test}

/**
  * Test for [[PushFilterIntoTableSourceScanRule]].
  */
class PushFilterIntoTableSourceScanRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(PushFilterIntoTableSourceScanRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE))
        .build()
    )

    // name: STRING, id: LONG, amount: INT, price: DOUBLE
    util.tableEnv.registerTableSource("MyTable", TestFilterableTableSource(true))
    val ddl =
      s"""
         |CREATE TABLE VirtualTable (
         |  name STRING,
         |  id bigint,
         |  amount int,
         |  virtualField as amount + 1,
         |  price double
         |) with (
         |  'connector.type' = 'TestFilterableSource',
         |  'is-bounded' = 'true'
         |)
       """.stripMargin
    util.tableEnv.sqlUpdate(ddl)
  }

  @Test
  def testCanPushDown(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE amount > 2")
  }

  @Test
  def testCanPushDownWithVirtualColumn(): Unit = {
    util.verifyPlan("SELECT * FROM VirtualTable WHERE amount > 2")
  }

  @Test
  def testCannotPushDown(): Unit = {
    // TestFilterableTableSource only accept predicates with `amount`
    util.verifyPlan("SELECT * FROM MyTable WHERE price > 10")
  }

  @Test
  def testCannotPushDownWithVirtualColumn(): Unit = {
    // TestFilterableTableSource only accept predicates with `amount`
    util.verifyPlan("SELECT * FROM VirtualTable WHERE price > 10")
  }

  @Test
  def testPartialPushDown(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE amount > 2 AND price > 10")
  }

  @Test
  def testPartialPushDownWithVirtualColumn(): Unit = {
    util.verifyPlan("SELECT * FROM VirtualTable WHERE amount > 2 AND price > 10")
  }

  @Test
  def testFullyPushDown(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE amount > 2 AND amount < 10")
  }

  @Test
  def testFullyPushDownWithVirtualColumn(): Unit = {
    util.verifyPlan("SELECT * FROM VirtualTable WHERE amount > 2 AND amount < 10")
  }

  @Test
  def testPartialPushDown2(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE amount > 2 OR price > 10")
  }

  @Test
  def testPartialPushDown2WithVirtualColumn(): Unit = {
    util.verifyPlan("SELECT * FROM VirtualTable WHERE amount > 2 OR price > 10")
  }

  @Test
  def testCannotPushDown3(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE amount > 2 OR amount < 10")
  }

  @Test
  def testCannotPushDown3WithVirtualColumn(): Unit = {
    util.verifyPlan("SELECT * FROM VirtualTable WHERE amount > 2 OR amount < 10")
  }

  @Test
  def testUnconvertedExpression(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM MyTable WHERE
        |    amount > 2 AND id < 100 AND CAST(amount AS BIGINT) > 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testWithUdf(): Unit = {
    util.addFunction("myUdf", Func1)
    util.verifyPlan("SELECT * FROM MyTable WHERE amount > 2 AND myUdf(amount) < 32")
  }

  @Test
  def testLowerUpperPushdown(): Unit = {
    val rti = new RowTypeInfo(Array[TypeInformation[_]](
      Types.STRING, Types.STRING),
      Array("a", "b"))
    val data = List(Row.of("foo", "bar"))
    util.tableEnv
      .registerTableSource(
        "MTable",
        TestFilterableTableSource(true, rti, data, Set("a", "b"))
      )

    util.verifyPlan("SELECT * FROM MTable WHERE LOWER(a) = 'foo' AND UPPER(b) = 'bar'")
  }
}

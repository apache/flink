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

import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.DateTimeTestUtil.localDateTime
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableConfigUtils, TestLegacyFilterableTableSource}
import org.apache.flink.types.Row

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
 * Test for [[PushFilterIntoLegacyTableSourceScanRule]].
 */
class PushFilterIntoLegacyTableSourceScanRuleTest
  extends PushFilterIntoTableSourceScanRuleTestBase {

  @Before
  def setup(): Unit = {
    util = batchTestUtil()
    util.asInstanceOf[BatchTableTestUtil].buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(PushFilterIntoLegacyTableSourceScanRule.INSTANCE,
          CoreRules.FILTER_PROJECT_TRANSPOSE))
        .build()
    )

    // name: STRING, id: LONG, amount: INT, price: DOUBLE
    TestLegacyFilterableTableSource.createTemporaryTable(
      util.tableEnv,
      TestLegacyFilterableTableSource.defaultSchema,
      "MyTable",
      isBounded = true)
    val ddl =
      """
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
    util.tableEnv.executeSql(ddl)
  }

  @Test
  override def testLowerUpperPushdown(): Unit = {
    val schema = TableSchema
      .builder()
      .field("a", DataTypes.STRING)
      .field("b", DataTypes.STRING)
      .build()

    val data = List(Row.of("foo", "bar"))
    TestLegacyFilterableTableSource.createTemporaryTable(
      util.tableEnv,
      schema,
      "MTable",
      isBounded = true,
      data,
      List("a", "b"))

    super.testLowerUpperPushdown()
  }

  @Test
  override def testWithInterval(): Unit = {
    val schema = TableSchema
      .builder()
      .field("a", DataTypes.TIMESTAMP)
      .field("b", DataTypes.TIMESTAMP)
      .build()

    val data = List(Row.of(
      localDateTime("2021-03-30 10:00:00"), localDateTime("2021-03-30 15:00:00")))
    TestLegacyFilterableTableSource.createTemporaryTable(
      util.tableEnv,
      schema,
      "MTable",
      isBounded = true,
      data,
      List("a", "b"))

    super.testWithInterval()
  }
}

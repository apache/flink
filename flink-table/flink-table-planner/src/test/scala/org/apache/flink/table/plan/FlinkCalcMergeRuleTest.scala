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

package org.apache.flink.table.plan

import org.apache.flink.api.scala._
import org.apache.flink.table.api.PlannerConfig
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfigBuilder
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalTableSourceScan}
import org.apache.flink.table.plan.rules.logical.FlinkCalcMergeRule
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil.{streamTableNode, term, unaryNode}

import org.apache.calcite.rel.rules.{FilterToCalcRule, ProjectToCalcRule}
import org.apache.calcite.tools.RuleSets
import org.junit.Test

import java.util.Random

class FlinkCalcMergeRuleTest extends TableTestBase {

  private val util = streamTestUtil()
  private val table = util.addTable[(Int, Int, String)]("MyTable", 'a, 'b, 'c)

  // rewrite distinct aggregate
  val cc: PlannerConfig = new CalciteConfigBuilder()
    .replaceNormRuleSet(RuleSets.ofList())
    .replaceLogicalOptRuleSet(RuleSets.ofList(
      FilterToCalcRule.INSTANCE,
      ProjectToCalcRule.INSTANCE,
      FlinkCalcMergeRule.INSTANCE,
      FlinkLogicalCalc.CONVERTER,
      FlinkLogicalTableSourceScan.CONVERTER
    ))
    .build()
  util.tableEnv.getConfig.setPlannerConfig(cc)

  @Test
  def testCalcMergeWithNonDeterministicExpr1(): Unit = {
    util.addFunction("random_udf", new NonDeterministicUdf)
    val sqlQuery = "SELECT a, a1 FROM (SELECT a, random_udf(a) AS a1 FROM MyTable) t WHERE a1 > 10"

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "a", "random_udf(a) AS a1")
      ),
      term("select", "a", "a1"),
      term("where", ">(a1, 10)")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testCalcMergeWithNonDeterministicExpr2(): Unit = {
    util.addFunction("random_udf", new NonDeterministicUdf)
    val sqlQuery = "SELECT a FROM (SELECT a FROM MyTable) t WHERE random_udf(a) > 10"

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(table),
          term("select", "a")
        ),
        term("select", "a"),
        term("where", ">(random_udf(a), 10)")
      ),
      term("select", "a")
    )

    util.verifySql(sqlQuery, expected)
  }
}

/**
  * Non-deterministic scalar function.
  */
class NonDeterministicUdf extends ScalarFunction {
  private val random = new Random

  def eval: Int = random.nextInt

  def eval(v: Int): Int = v + random.nextInt

  override def isDeterministic = false
}
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

package org.apache.flink.table.api.batch.table

import org.apache.calcite.rel.rules.{CalcMergeRule, FilterCalcMergeRule, ProjectCalcMergeRule}
import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{TableFunc0, TableFunc1, TableTestBase}
import org.junit.Test

import scala.collection.JavaConversions._

class CorrelateTest extends TableTestBase {

  @Test
  def testCrossJoin(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result1 = table.join(function('c) as 's).select('c, 's)

    val expected1 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("correlate", s"table(${function.getClass.getSimpleName}(c))"),
        term("select", "a", "b", "c", "s"),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(65536) c, VARCHAR(65536) s)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "s")
    )

    util.verifyTable(result1, expected1)

    // test overloading

    val result2 = table.join(function('c, "$") as 's).select('c, 's)

    val expected2 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2, '$$')"),
        term("correlate", s"table(${function.getClass.getSimpleName}(c, '$$'))"),
        term("select", "a", "b", "c", "s"),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(65536) c, VARCHAR(65536) s)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "s")
    )

    util.verifyTable(result2, expected2)
  }

  @Test
  def testLeftOuterJoinWithoutJoinPredicates(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result = table.leftOuterJoin(function('c) as 's).select('c, 's).where('s > "")

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("correlate", s"table(${function.getClass.getSimpleName}(c))"),
        term("select", "a", "b", "c", "s"),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(65536) c, VARCHAR(65536) s)"),
        term("joinType", "LEFT")
      ),
      term("select", "c", "s"),
      term("where", ">(s, '')")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testLeftOuterJoinWithLiteralTrue(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result = table.leftOuterJoin(function('c) as 's, true).select('c, 's)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("correlate", s"table(${function.getClass.getSimpleName}(c))"),
        term("select", "a", "b", "c", "s"),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(65536) c, VARCHAR(65536) s)"),
        term("joinType", "LEFT")
      ),
      term("select", "c", "s"))

    util.verifyTable(result, expected)
  }

  @Test
  def testCorrelateWithMultiFilter(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc0)

    val result = sourceTable.select('a, 'b, 'c)
      .join(function('c) as('d, 'e))
      .select('c, 'd, 'e)
      .where('e > 10)
      .where('e > 20)
      .select('c, 'd)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("correlate", s"table(${function.getClass.getSimpleName}(c))"),
        term("select", "a", "b", "c", "d", "e"),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(65536) c, " +
               "VARCHAR(65536) d, INTEGER e)"),
        term("joinType", "INNER"),
        term("condition", "AND(>($1, 10), >($1, 20))")
      ),
      term("select", "c", "d")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testCorrelateWithMultiFilterAndWithoutCalcMergeRules(): Unit = {
    val util = batchTestUtil()

    val logicalRuleSet = FlinkRuleSets.LOGICAL_OPT_RULES.filter {
        case CalcMergeRule.INSTANCE => false
        case FilterCalcMergeRule.INSTANCE => false
        case ProjectCalcMergeRule.INSTANCE => false
        case _ => true
      }

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceLogicalOptRuleSet(RuleSets.ofList(logicalRuleSet.toList))
      .build()

    util.tableEnv.getConfig.setCalciteConfig(cc)

    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc0)

    val result = sourceTable.select('a, 'b, 'c)
      .join(function('c) as('d, 'e))
      .select('c, 'd, 'e)
      .where('e > 10)
      .where('e > 20)
      .select('c, 'd)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("correlate", s"table(${function.getClass.getSimpleName}(c))"),
        term("select", "a", "b", "c", "d", "e"),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(65536) c, " +
               "VARCHAR(65536) d, INTEGER e)"),
        term("joinType", "INNER"),
        term("condition", "AND(>($1, 10), >($1, 20))")
      ),
      term("select", "c", "d")
    )

    util.verifyTable(result, expected)
  }
}

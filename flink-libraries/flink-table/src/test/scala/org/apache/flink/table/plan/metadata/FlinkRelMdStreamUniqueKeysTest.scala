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

package org.apache.flink.table.plan.metadata

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.stream.sql.AddUdf
import org.apache.flink.table.plan.optimize.FlinkStreamPrograms
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.runtime.utils.StreamTestData
import org.apache.flink.table.util.{TableTestBase, TestFlinkLogicalLastRowRule, TestTableSourceWithUniqueKeys}

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet

import org.apache.calcite.tools.RuleSets
import org.junit.Test

class FlinkRelMdStreamUniqueKeysTest extends TableTestBase {

  @Test
  def testSimpleSqlWithoutUniqueKeys(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val sql = s"SELECT a, b, c FROM $table WHERE b > 12"
    util.verifyUniqueKeys(sql, Set().asInstanceOf[Set[Int]])
  }

  @Test
  def testUnionSqlWithoutUniqueKeys(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val table2 = util.addTable[(Long, Int, String)]('d, 'e, 'f)
    val sql = s"SELECT d, e, f FROM $table2 UNION ALL SELECT a, b, c FROM $table1"
    util.verifyUniqueKeys(sql, Set().asInstanceOf[Set[Int]])
  }

  @Test
  def testGroupbyWithoutWindow(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Int)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    val sql1 = "SELECT COUNT(a) FROM MyTable GROUP BY b"
    util.verifyUniqueKeys(sql1)
    val sql2 = "SELECT COUNT(a), b FROM MyTable GROUP BY b"
    util.verifyUniqueKeys(sql2, Set(1))
    val sql3 = "SELECT COUNT(a), b , b AS c, CAST(b AS DOUBLE) FROM MyTable GROUP BY b"
    util.verifyUniqueKeys(sql3, Set(1), Set(2))
    val sql4 = "SELECT COUNT(a), b , c, c FROM MyTable GROUP BY b, c"
    util.verifyUniqueKeys(sql4, Set(1, 2), Set(1, 3))
    val sql5 = "SELECT COUNT(a), b , c, c, CAST(c AS Int) FROM MyTable GROUP BY b, c"
    util.verifyUniqueKeys(sql5, Set(1, 2), Set(1, 3), Set(1, 4))

  }

  @Test
  def testGroupbyWithWindow(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    util.addFunction("weightedAvg", new WeightedAvgWithMerge)
    val sql =
      "SELECT " +
        "  COUNT(*), weightedAvg(c, a) AS wAvg, " +
        "  TUMBLE_START(rowtime, INTERVAL '15' MINUTE), " +
        "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE)" +
        "FROM MyTable " +
        "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"
    util.verifyUniqueKeys(sql, Set(2), Set(3))
  }

  @Test
  def testTopN(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)
    util.addFunction("add", new AddUdf)
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= 10
      """.stripMargin
    util.verifyUniqueKeys(sql, Set(0, 3))
  }

  @Test
  def testSourceWithPk(): Unit = {
    val util = streamTestUtil()

    val rowTypeInfo = new RowTypeInfo(Types.INT, Types.LONG, Types.STRING)

    util.tableEnv.registerTableSource("MyTable", new TestTableSourceWithUniqueKeys(
      StreamTestData.get3TupleData,
      Array("a", "pk", "c"),
      Array(0, 1, 2),
      ImmutableSet.of(ImmutableSet.copyOf(Array[String]("pk")))
    )(rowTypeInfo.asInstanceOf[TypeInformation[(Int, Long, String)]]))

    injectRules(
      util.tableEnv,
      FlinkStreamPrograms.LOGICAL_REWRITE,
      RuleSets.ofList(TestFlinkLogicalLastRowRule.INSTANCE))

    val resultTable = util.tableEnv.scan("MyTable")
        .select('c, 'pk)
    util.verifyUniqueKeys(resultTable, Set(1))
  }
}

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

package org.apache.flink.table.api.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class ExplainTest(extended: Boolean) extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
  util.addDataStream[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
  util.addDataStream[(Int, Long, String)]("MyTable2", 'd, 'e, 'f)

  val STRING = new VarCharType(VarCharType.MAX_LENGTH)
  val LONG = new BigIntType()
  val INT = new IntType()

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4)
  }

  @Test
  def testExplainWithTableSourceScan(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable", extended)
  }

  @Test
  def testExplainWithDataStreamScan(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1", extended)
  }

  @Test
  def testExplainWithFilter(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 WHERE mod(a, 2) = 0", extended)
  }

  @Test
  def testExplainWithAgg(): Unit = {
    util.verifyExplain("SELECT COUNT(*) FROM MyTable1 GROUP BY a", extended)
  }

  @Test
  def testExplainWithJoin(): Unit = {
    // TODO support other join operators when them are supported
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")
    util.verifyExplain("SELECT a, b, c, e, f FROM MyTable1, MyTable2 WHERE a = d", extended)
  }

  @Test
  def testExplainWithUnion(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 UNION ALL SELECT * FROM MyTable2", extended)
  }

  @Test
  def testExplainWithSort(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 ORDER BY a LIMIT 5", extended)
  }

  @Test
  def testExplainWithSingleSink(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT * FROM MyTable1 WHERE a > 10")
    val sink = util.createCollectTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.writeToSink(table, sink, "sink")
    util.verifyExplain(extended)
  }

  @Test
  def testExplainWithMultiSinks(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT a, COUNT(*) AS cnt FROM MyTable1 GROUP BY a")
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery("SELECT * FROM TempTable WHERE cnt > 10")
    val sink1 = util.createCollectTableSink(Array("a", "cnt"), Array(INT, LONG))
    util.writeToSink(table1, sink1, "sink1")

    val table2 = util.tableEnv.sqlQuery("SELECT * FROM TempTable WHERE cnt < 10")
    val sink2 = util.createCollectTableSink(Array("a", "cnt"), Array(INT, LONG))
    util.writeToSink(table2, sink2, "sink2")

    util.verifyExplain(extended)
  }

}

object ExplainTest {
  @Parameterized.Parameters(name = "extended={0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}

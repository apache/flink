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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.NodeResourceUtil

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import scala.collection.Seq

@RunWith(classOf[Parameterized])
class SubplanReuseITCase(SubplanReuse: Boolean) extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.setConf(new Configuration)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, SubplanReuse)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, false)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MEM, 32)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM, 32)
    tEnv.getConfig.getConf.setInteger(NodeResourceUtil.SQL_RESOURCE_PER_REQUEST_MEM, 2)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_HASH_JOIN_TABLE_MEM, 5)

    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfSmallData3)
    registerCollection("SmallTable5", smallData5, type5, "a, b, c, d, e", nullablesOfSmallData5)
    tEnv.registerTableSource("x", CommonTestData.getSmall3Source(Array("a", "b", "c")))
    tEnv.registerTableSource("y", CommonTestData.getSmall5Source(Array("a", "b", "c", "d", "e")))
  }

  @Test
  def testSubplanReuse_Calc(): Unit = {
    checkResult(
      """
        |WITH r AS (SELECT a, b, c FROM x WHERE c LIKE 'He%')
        |(SELECT r.a, y.e FROM r, y WHERE r.a = y.a)
        |UNION ALL
        |(SELECT r.a, y.e FROM r, y WHERE r.a = y.a)
      """.stripMargin,
      Seq(row(2, 1), row(2, 1), row(2, 2), row(2, 2))
    )
  }

  @Test
  def testSubplanReuse_Exchange(): Unit = {
    checkResult(
      """
        |WITH r AS (SELECT a, b, c FROM x WHERE c LIKE 'He%')
        |SELECT * FROM r, y WHERE r.a = y.a AND y.e > 1
        |UNION ALL
        |SELECT * FROM r, y WHERE r.a = y.c AND d <> ''
      """.stripMargin,
      Seq(row(2, 2, "Hello", 2, 2, 1, "Hallo Welt", 2),
        row(2, 2, "Hello", 2, 3, 2, "Hallo Welt wie", 1))
    )
  }

  @Test
  def testSubplanReuse_OverWindow(): Unit = {
    checkResult(
      """
        |WITH r AS (SELECT a, b, RANK() OVER (ORDER BY c DESC) c FROM x)
        |SELECT r1.a, r1.b, r1.c, r2.b, r2.c FROM r r1, r r2
        | WHERE r1.a = r2.a AND r1.b < 10 AND r2.b > 1
      """.stripMargin,
      Seq(row(2, 2, 3, 2, 3), row(3, 2, 2, 2, 2))
    )
  }

  @Test
  def testSubplanReuse_ScanTable(): Unit = {
    checkResult(
      """
        |(SELECT a FROM SmallTable3 WHERE b > 1)
        |UNION ALL
        |(SELECT a FROM SmallTable3 WHERE b < 2)
      """.stripMargin,
      Seq(row(1), row(2), row(3))
    )
  }

  @Test
  def testSubplanReuse_HashAggregate(): Unit = {
    tEnv.getConfig.getConf.setString(TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "SortAgg")
    checkResult(
      """
        |WITH r AS (SELECT b, SUM(a) a, SUM(e) e FROM y GROUP BY b)
        |SELECT r1.a, r1.e, r2.a FROM r r1, r r2 WHERE r1.a = r2.e AND r2.a > 1
      """.stripMargin,
      Seq(row(1, 1, 2), row(2, 1, 2), row(2, 2, 2))
    )
  }

  @Test
  def testSubplanReuse_SortAggregate(): Unit = {
    tEnv.getConfig.getConf.setString(TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "HashAgg")
    checkResult(
      """
        |WITH r AS (SELECT b, SUM(a) a, SUM(e) e FROM y GROUP BY b)
        |SELECT r1.a, r1.e, r2.a FROM r r1, r r2 WHERE r1.a = r2.e AND r2.a > 1
      """.stripMargin,
      Seq(row(1, 1, 2), row(2, 1, 2), row(2, 2, 2))
    )
  }

  @Test
  def testSubplanReuse_Sort(): Unit = {
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, true)
    checkResult(
      """
        |WITH r AS (SELECT b, SUM(a) a, SUM(e) e FROM y GROUP BY b ORDER BY a, e DESC)
        |SELECT r1.a, r1.e, r2.a FROM r r1, r r2 WHERE r1.a = r2.e AND r2.a > 1 AND r1.a < 3
      """.stripMargin,
      Seq(row(1, 1, 2), row(2, 1, 2), row(2, 2, 2))
    )
  }

  @Test
  def testSubplanReuse_Limit(): Unit = {
    checkResult(
      """
        |WITH r AS (SELECT a, b FROM x LIMIT 10)
        |SELECT r1.a, r1.b, r2.a FROM r r1, r r2 WHERE r1.a = r2.b
      """.stripMargin,
      Seq(row(1, 1, 1), row(2, 2, 2), row(2, 2, 3))
    )
  }

  @Test
  def testSubplanReuse_SortLimit(): Unit = {
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, true)
    checkResult(
      """
        |WITH r AS (SELECT b, SUM(a) a, SUM(e) e FROM y GROUP BY b ORDER BY a, e DESC LIMIT 5)
        |SELECT r1.a, r1.e, r2.a FROM r r1, r r2 WHERE r1.a = r2.e AND r2.a > 1 AND r1.a < 3
      """.stripMargin,
      Seq(row(1, 1, 2), row(2, 1, 2), row(2, 2, 2))
    )
  }

  @Test
  def testSubplanReuse_SortMergeJoin(): Unit = {
    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")
    checkResult(
      """
        |WITH r AS (SELECT x.a as xa, x.b xb, y.a ya, y.b yb, y.e ye FROM x, y
        |  WHERE x.a = y.a AND x.c LIKE 'H%')
        |SELECT * FROM r r1, r r2
        |WHERE r1.xb = r2.ye AND (r1.xa > 1 or r2.yb > 2)
      """.stripMargin,
      Seq(row(1, 1, 1, 1, 1, 2, 2, 2, 3, 1),
        row(2, 2, 2, 2, 2, 2, 2, 2, 2, 2),
        row(2, 2, 2, 3, 1, 2, 2, 2, 2, 2))
    )
  }

  @Test
  def testSubplanReuse_HashJoin(): Unit = {
    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    checkResult(
      """
        |WITH r AS (SELECT x.a as xa, x.b xb, y.a ya, y.b yb, y.e ye FROM x, y
        |  WHERE x.a = y.a AND x.c LIKE 'H%')
        |SELECT * FROM r r1, r r2
        |WHERE r1.xb = r2.ye AND (r1.xa > 1 or r2.yb > 2)
      """.stripMargin,
      Seq(row(1, 1, 1, 1, 1, 2, 2, 2, 3, 1),
        row(2, 2, 2, 2, 2, 2, 2, 2, 2, 2),
        row(2, 2, 2, 3, 1, 2, 2, 2, 2, 2))
    )
  }

  @Test
  def testSubplanReuse_NestedLoopJoin(): Unit = {
    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    checkResult(
      """
        |WITH r AS (SELECT x.a as xa, x.b xb, y.a ya, y.b yb, y.e ye FROM x, y
        |  WHERE x.a = y.a AND x.c LIKE 'H%')
        |SELECT * FROM r r1, r r2
        |WHERE r1.xb = r2.ye AND (r1.xa > 1 or r2.yb > 2)
      """.stripMargin,
      Seq(row(1, 1, 1, 1, 1, 2, 2, 2, 3, 1),
        row(2, 2, 2, 2, 2, 2, 2, 2, 2, 2),
        row(2, 2, 2, 3, 1, 2, 2, 2, 2, 2))
    )
  }

  @Test
  def testSubplanReuse_Union(): Unit = {
    checkResult(
      """
        |WITH r AS (SELECT a, c FROM x WHERE b > 1 UNION ALL SELECT a, d FROM y WHERE b < 2)
        |SELECT r1.a, r2.c FROM r r1, r r2 WHERE r1.a = r2.a
      """.stripMargin,
      Seq(row(1, "Hallo"), row(2, "Hello"), row(3, "Hello world"))
    )
  }

  @Test
  def testEnableReuseTableSource(): Unit = {
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, true)
    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    checkResult(
      """
        |WITH t AS (SELECT x.a AS xa, x.b AS xb, y.a AS ya, y.e AS ye FROM x, y WHERE x.a = y.a)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.xb = t2.ye AND t1.xa < 3 AND t2.xa > 1
      """.stripMargin,
      Seq(row(1, 1, 1, 1, 2, 2, 2, 1), row(2, 2, 2, 1, 2, 2, 2, 2), row(2, 2, 2, 2, 2, 2, 2, 2))
    )
  }

  @Test
  def testDisableReuseTableSource(): Unit = {
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, false)
    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    checkResult(
      """
        |WITH t AS (SELECT x.a AS xa, x.b AS xb, y.a AS ya, y.e AS ye FROM x, y WHERE x.a = y.a)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.xb = t2.ye AND t1.xa < 3 AND t2.xa > 1
      """.stripMargin,
      Seq(row(1, 1, 1, 1, 2, 2, 2, 1), row(2, 2, 2, 1, 2, 2, 2, 2), row(2, 2, 2, 2, 2, 2, 2, 2))
    )
  }

  @Test
  def testNestedReusableSubplan(): Unit = {
    checkResult(
      """
        |WITH v1 AS (
        | SELECT
        |   y.b AS yb,
        |   SUM(x.b) sum_b,
        |   AVG(SUM(x.b)) OVER (PARTITION BY y.b) avg_b,
        |   RANK() OVER (PARTITION BY y.b ORDER BY y.b) rn,
        |   y.b
        | FROM x, y
        | WHERE x.a = y.a AND x.c IS NOT NULl AND y.c < 2
        | GROUP BY y.b
        |),
        |   v2 AS (
        | SELECT
        |    v11.yb,
        |    v11.avg_b,
        |    v11.sum_b,
        |    v12.sum_b psum,
        |    v13.sum_b nsum,
        |    v12.avg_b avg_b2
        |  FROM v1 v11, v1 v12, v1 v13
        |  WHERE v11.yb = v12.yb AND v11.yb = v13.yb AND
        |    v11.rn = v12.rn AND v11.rn = v13.rn
        |)
        |SELECT * from v2 WHERE sum_b > 0 AND avg_b > 0
      """.stripMargin,
      Seq(row(1, 1.0, 1, 1, 1, 1.0), row(2, 2.0, 2, 2, 2, 2.0))
    )
  }
}

object SubplanReuseITCase {

  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}

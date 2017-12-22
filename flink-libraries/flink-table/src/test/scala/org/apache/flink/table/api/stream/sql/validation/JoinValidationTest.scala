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

package org.apache.flink.table.api.stream.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class JoinValidationTest extends TableTestBase {

  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c.rowtime, 'proctime.proctime)
  streamUtil.addTable[(Int, String, Long)]("MyTable2", 'a, 'b, 'c.rowtime, 'proctime.proctime)

  /** There should exist exactly two time conditions **/
  @Test(expected = classOf[TableException])
  def testWindowJoinSingleTimeCondition() = {
    val sql =
      """
        |SELECT t2.a
        |FROM MyTable t1 JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime > t2.proctime - INTERVAL '5' SECOND""".stripMargin
    streamUtil.verifySql(sql, "n/a")
  }

  /** Both time attributes in a join condition must be of the same type **/
  @Test(expected = classOf[TableException])
  def testWindowJoinDiffTimeIndicator() = {
    val sql =
      """
        |SELECT t2.a FROM
        |MyTable t1 JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime > t2.proctime - INTERVAL '5' SECOND AND
        |  t1.proctime < t2.c + INTERVAL '5' SECOND""".stripMargin
    streamUtil.verifySql(sql, "n/a")
  }

  /** The time conditions should be an And condition **/
  @Test(expected = classOf[TableException])
  def testWindowJoinNotCnfCondition() = {
    val sql =
      """
        |SELECT t2.a
        |FROM MyTable t1 JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  (t1.proctime > t2.proctime - INTERVAL '5' SECOND OR
        |   t1.proctime < t2.c + INTERVAL '5' SECOND)""".stripMargin
    streamUtil.verifySql(sql, "n/a")
  }

  /** Validates that no rowtime attribute is in the output schema **/
  @Test(expected = classOf[TableException])
  def testNoRowtimeAttributeInResult(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND t2.proctime
        | """.stripMargin

    streamUtil.verifySql(sql, "n/a")
  }

  /** Validates that range and equality predicate are not accepted **/
  @Test(expected = classOf[TableException])
  def testRangeAndEqualityPredicates(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |  t1.proctime > t2.proctime - INTERVAL '5' SECOND AND
        |  t1.proctime = t2.proctime
        | """.stripMargin

    streamUtil.verifySql(sql, "n/a")
  }

  /** Validates that equality predicate with offset are not accepted **/
  @Test(expected = classOf[TableException])
  def testEqualityPredicateWithOffset(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |  t1.proctime = t2.proctime - INTERVAL '5' SECOND
        | """.stripMargin

    streamUtil.verifySql(sql, "n/a")
  }

  /** Validates that no rowtime attribute is in the output schema for non-window inner join **/
  @Test(expected = classOf[TableException])
  def testNoRowtimeAttributeInResultForNonWindowInnerJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a
        | """.stripMargin

    streamUtil.verifySql(sql, "n/a")
  }

  /** Validates that no proctime attribute is in remaining predicate for non-window inner join **/
  @Test(expected = classOf[TableException])
  def testNoProctimeAttributeInResultForNonWindowInnerJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND t1.proctime > t2.proctime
        | """.stripMargin

    streamUtil.verifySql(sql, "n/a")
  }
}

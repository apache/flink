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

package org.apache.flink.table.plan.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.{DATE, TIMESTAMP}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{Table, TableException}
import org.apache.flink.table.plan.optimize.{BatchOptimizeContext, FlinkChainedPrograms, FlinkCorrelateVariablesValidationProgram}
import org.apache.flink.table.plan.rules.logical.SubQueryTestBase
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.util.TestTableSourceWithFieldNullables

import scala.collection.Seq

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
  * Test for checking correlate variables exist after decorrelate.
  * @param fieldsNullable if fields can be nullable
  */
@RunWith(classOf[Parameterized])
class SubqueryCorrelateVariablesValidationTest(fieldsNullable: Boolean)
  extends SubQueryTestBase(fieldsNullable){

  private lazy val tableSchema = Seq[TypeInformation[_]](
    STRING_TYPE_INFO,
    SHORT_TYPE_INFO,
    INT_TYPE_INFO,
    LONG_TYPE_INFO,
    FLOAT_TYPE_INFO,
    DOUBLE_TYPE_INFO,
    BIG_DEC_TYPE_INFO,
    TIMESTAMP,
    DATE).toArray

  private lazy val defaultNullableSeq = Array.fill(tableSchema.length)(fieldsNullable)

  private val tableNames = Seq("t1", "t2", "t3")

  @Before
  def before(): Unit = {
    for(name <- tableNames) {
      addTable(name)
    }
  }

  @Test(expected = classOf[RuntimeException])
  def testWithProjectProjectCorrelate(): Unit = {
    val testSql =
      """
        |SELECT (SELECT min(t1.t1d) FROM t3 WHERE t3.t3a = 'test') min_t1d
        |FROM   t1
        |WHERE  t1a = 'test'
      """.stripMargin
    util.printSql(testSql)
  }

  @Test(expected = classOf[RuntimeException])
  def testWithProjectFilterCorrelate(): Unit = {
    val testSql =
      """
        |SELECT (SELECT min(t3d) FROM t3 WHERE t3.t3a = t1.t1a) min_t3d,
        |       (SELECT max(t2h) FROM t2 WHERE t2.t2a = t1.t1a) max_t2h
        |FROM   t1
        |WHERE  t1a = 'test'
      """.stripMargin
    util.printSql(testSql)
  }

  @Test(expected = classOf[RuntimeException])
  def testWithProjectJoinCorrelate(): Unit = {
    val testSql =
      """
        |SELECT (SELECT max(t2h) FROM t2
        |LEFT OUTER JOIN t1 ttt
        |ON t2.t2a=t1.t1a) max_t2h
        |FROM   t1
        |WHERE  t1a = 'val1b'
      """.stripMargin
    util.printSql(testSql)
  }

  @Test(expected = classOf[TableException])
  def testWithFilterJoinCorrelate(): Unit = {
    val testSql =
      """
        |SELECT t1a
        |FROM   t1
        |WHERE  EXISTS (SELECT max(t2h) FROM t2
        |               LEFT OUTER JOIN t1 ttt
        |               ON t2.t2a=t1.t1a)
      """.stripMargin
    util.printSql(testSql)
  }

  @Test(expected = classOf[TableException])
  def testWithFilterInCorrelate(): Unit = {
    val testSql =
      """
        |SELECT * FROM t1
        |WHERE t1a
        |IN (SELECT t3a
        |    FROM t3
        |    WHERE t1.t1e
        |    IN (select t2e from t2))
      """.stripMargin
    util.printSql(testSql)
  }

  @Test(expected = classOf[TableException])
  def testWithFilterExistsCorrelate(): Unit = {
    val testSql =
      """
        |SELECT *
        |FROM t1
        |WHERE EXISTS (SELECT *
        |              FROM t3
        |              WHERE EXISTS(select * from t3 WHERE t1.t1a = t3.t3a))
      """.stripMargin
    util.printSql(testSql)
  }

  @Test(expected = classOf[RuntimeException])
  def testWithProjectCaseWhenCorrelate(): Unit = {
    val testSql =
      """
        |SELECT
        |    (CASE WHEN EXISTS (SELECT min(t3d)
        |                       FROM t3
        |                       WHERE t3.t3a = t1.t1a)
        |     THEN 1 ELSE 2 END)
        |FROM   t1
        |WHERE  t1a = 'test'
      """.stripMargin
    util.printSql(testSql)
  }

  private def addTable(tableName: String): Unit = {
    val ts = new TestTableSourceWithFieldNullables(fieldList(tableName),
      tableSchema, defaultNullableSeq)
    util.getTableEnv.registerTableSource(tableName, ts)
    util.getTableEnv.scan(tableName)
  }

  private def fieldList(tableName: String): Array[String] = {
    val columnSuffix = "a,b,c,d,e,f,g,h,i".split(",")
    val fields = new Array[String](columnSuffix.length)
    columnSuffix.zipWithIndex.foreach(item => fields(item._2) = tableName + item._1)
    fields
  }

  override def buildPrograms(): FlinkChainedPrograms[BatchOptimizeContext] = {
    val programs = super.buildPrograms()
    programs.addLast("correlate_variables_validation", new FlinkCorrelateVariablesValidationProgram)
    programs
  }
}

object SubqueryCorrelateVariablesValidationTest {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true)
  }
}

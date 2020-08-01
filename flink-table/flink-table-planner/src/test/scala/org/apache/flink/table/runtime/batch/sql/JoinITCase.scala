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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.runtime.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class JoinITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testInnerJoin(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("d", "e", "f", "g", "h")
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInnerJoinWithFilter(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e AND b < 2"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("d", "e", "f", "g", "h")
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hi,Hallo\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6 AND h < b"

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("d", "e", "f", "g", "h")
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hello world, how are you?,Hallo Welt wie\n" + "I am fine.,Hallo Welt wie\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInnerJoinWithMultipleKeys(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE a = d AND b = h"

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("d", "e", "f", "g", "h")
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt wie gehts?\n" +
      "Hello world,ABC\n" + "I am fine.,HIJ\n" + "I am fine.,IJK\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInnerJoinWithAlias(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val sqlQuery =
      "SELECT Table5.c, T.`1-_./Ü` FROM (SELECT a, b, c AS `1-_./Ü` FROM Table3) AS T, Table5 " +
      "WHERE a = d AND a < 4"

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("d", "e", "f", "g", "c")
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)
    val expected = "1,Hi\n" + "2,Hello\n" + "1,Hello\n" +
      "2,Hello world\n" + "2,Hello world\n" + "3,Hello world\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInnerJoinWithAggregation(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val sqlQuery = "SELECT COUNT(g), COUNT(b) FROM Table3, Table5 WHERE a = d"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    tEnv.createTemporaryView("Table3", ds1, 'a, 'b, 'c)
    tEnv.createTemporaryView("Table5", ds2, 'd, 'e, 'f, 'g, 'h)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "6,6"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInnerJoinWithAggregation2(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val sqlQuery = "SELECT COUNT(b), COUNT(g) FROM Table3, Table5 WHERE a = d"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "6,6"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFullOuterJoin(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    tEnv.getConfig.setNullCheck(true)

    val sqlQuery = "SELECT c, g FROM Table3 FULL OUTER JOIN Table5 ON b = e"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("d", "e", "f", "g", "h")
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n" +
      "null,Hallo Welt wie\n" + "null,Hallo Welt wie gehts?\n" + "null,ABC\n" + "null,BCD\n" +
      "null,CDE\n" + "null,DEF\n" + "null,EFG\n" + "null,FGH\n" + "null,GHI\n" + "null,HIJ\n" +
      "null,IJK\n" + "null,JKL\n" + "null,KLM"

    val results = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testLeftOuterJoin(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    tEnv.getConfig.setNullCheck(true)

    val sqlQuery = "SELECT c, g FROM Table5 LEFT OUTER JOIN Table3 ON b = e"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("d", "e", "f", "g", "h")
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n" +
      "null,Hallo Welt wie\n" + "null,Hallo Welt wie gehts?\n" + "null,ABC\n" + "null,BCD\n" +
      "null,CDE\n" + "null,DEF\n" + "null,EFG\n" + "null,FGH\n" + "null,GHI\n" + "null,HIJ\n" +
      "null,IJK\n" + "null,JKL\n" + "null,KLM"
    val results = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRightOuterJoin(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    tEnv.getConfig.setNullCheck(true)

    val sqlQuery = "SELECT c, g FROM Table3 RIGHT OUTER JOIN Table5 ON b = e"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("d", "e", "f", "g", "h")
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n" +
      "null,Hallo Welt wie\n" + "null,Hallo Welt wie gehts?\n" + "null,ABC\n" + "null,BCD\n" +
      "null,CDE\n" + "null,DEF\n" + "null,EFG\n" + "null,FGH\n" + "null,GHI\n" + "null,HIJ\n" +
      "null,IJK\n" + "null,JKL\n" + "null,KLM"
    val results = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCrossJoinWithLeftSingleRowInput(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val table = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a1", "a2", "a3")
    tEnv.registerTable("A", table)

    val sqlQuery2 = "SELECT * FROM (SELECT count(*) FROM A) CROSS JOIN A"
    val expected =
      "3,1,1,Hi\n" +
      "3,2,2,Hello\n" +
      "3,3,2,Hello world"
    val result = tEnv.sqlQuery(sqlQuery2).collect()
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testCrossJoinWithRightSingleRowInput(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val table = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a1", "a2", "a3")
    tEnv.registerTable("A", table)

    val sqlQuery1 = "SELECT * FROM A CROSS JOIN (SELECT count(*) FROM A)"
    val expected =
      "1,1,Hi,3\n" +
      "2,2,Hello,3\n" +
      "3,2,Hello world,3"
    val result = tEnv.sqlQuery(sqlQuery1).collect()
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testCrossJoinWithEmptySingleRowInput(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val table = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a1", "a2", "a3")
    tEnv.registerTable("A", table)

    val sqlQuery1 = "SELECT * FROM A CROSS JOIN (SELECT count(*) FROM A HAVING count(*) < 0)"
    val result = tEnv.sqlQuery(sqlQuery1).count()
    Assert.assertEquals(0, result)
  }

  @Test
  def testLeftNullRightJoin(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val sqlQuery =
      "SELECT a, cnt " +
      "FROM (SELECT cnt FROM (SELECT COUNT(*) AS cnt FROM B) WHERE cnt < 0) RIGHT JOIN A ON a < cnt"

    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("a", "b", "c", "d", "e")
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("A", ds1)
    tEnv.registerTable("B", ds2)


    val result = tEnv.sqlQuery(sqlQuery)
    val expected = Seq(
          "1,null",
          "2,null", "2,null",
          "3,null", "3,null", "3,null",
          "4,null", "4,null", "4,null", "4,null",
          "5,null", "5,null", "5,null", "5,null", "5,null").mkString("\n")

    val results = result.toDataSet[Row].collect()

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Test
  def testLeftSingleRightJoinEqualPredicate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val sqlQuery =
      "SELECT a, cnt FROM (SELECT COUNT(*) AS cnt FROM B) RIGHT JOIN A ON cnt = a"

    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("a", "b", "c", "d", "e")
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("A", ds1)
    tEnv.registerTable("B", ds2)

    val result = tEnv.sqlQuery(sqlQuery)
    val expected = Seq(
      "1,null", "2,null", "2,null", "3,3", "3,3",
      "3,3", "4,null", "4,null", "4,null",
      "4,null", "5,null", "5,null", "5,null",
      "5,null", "5,null").mkString("\n")

    val results = result.toDataSet[Row].collect()

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testLeftSingleRightJoinNotEqualPredicate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val sqlQuery =
      "SELECT a, cnt FROM (SELECT COUNT(*) AS cnt FROM B) RIGHT JOIN A ON cnt > a"

    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("a", "b", "c", "d", "e")
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("A", ds1)
    tEnv.registerTable("B", ds2)

    val result = tEnv.sqlQuery(sqlQuery)
    val expected = Seq(
      "1,3", "2,3", "2,3", "3,null", "3,null",
      "3,null", "4,null", "4,null", "4,null",
      "4,null", "5,null", "5,null", "5,null",
      "5,null", "5,null").mkString("\n")

    val results = result.toDataSet[Row].collect()

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRightNullLeftJoin(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val sqlQuery =
      "SELECT a, cnt " +
      "FROM A LEFT JOIN (SELECT cnt FROM (SELECT COUNT(*) AS cnt FROM B) WHERE cnt < 0) ON cnt > a"

    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("A", ds2)
    tEnv.registerTable("B", ds1)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq(
      "2,null", "3,null", "1,null").mkString("\n")

    val results = result.toDataSet[Row].collect()

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRightSingleLeftJoinEqualPredicate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val sqlQuery =
      "SELECT a, cnt FROM A LEFT JOIN (SELECT COUNT(*) AS cnt FROM B) ON cnt = a"

    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("a", "b", "c", "d", "e")
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("A", ds1)
    tEnv.registerTable("B", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq(
      "1,null", "2,null", "2,null", "3,3", "3,3",
      "3,3", "4,null", "4,null", "4,null",
      "4,null", "5,null", "5,null", "5,null",
      "5,null", "5,null").mkString("\n")

    val results = result.toDataSet[Row].collect()

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRightSingleLeftJoinNotEqualPredicate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val sqlQuery =
      "SELECT a, cnt FROM A LEFT JOIN (SELECT COUNT(*) AS cnt FROM B) ON cnt < a"

    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("a", "b", "c", "d", "e")
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("A", ds1)
    tEnv.registerTable("B", ds2)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = Seq(
      "1,null", "2,null", "2,null", "3,null", "3,null",
      "3,null", "4,3", "4,3", "4,3",
      "4,3", "5,3", "5,3", "5,3",
      "5,3", "5,3").mkString("\n")

    val results = result.toDataSet[Row].collect()

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRightSingleLeftJoinTwoFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val sqlQuery =
      "SELECT a, cnt, cnt2 " +
      "FROM t1 LEFT JOIN (SELECT COUNT(*) AS cnt,COUNT(*) AS cnt2 FROM t2 ) AS x ON a = cnt"

    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as("a", "b", "c", "d", "e")
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("t1", ds1)
    tEnv.registerTable("t2", ds2)

    val result = tEnv.sqlQuery(sqlQuery)
    val expected = Seq(
      "1,null,null",
      "2,null,null", "2,null,null",
      "3,3,3", "3,3,3", "3,3,3",
      "4,null,null", "4,null,null", "4,null,null", "4,null,null",
      "5,null,null", "5,null,null", "5,null,null", "5,null,null", "5,null,null").mkString("\n")

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCrossWithUnnest(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val data = List(
      (1, 1L, Array("Hi", "w")),
      (2, 2L, Array("Hello", "k")),
      (3, 2L, Array("Hello world", "x"))
    )
    val stream = env.fromCollection(data)
    tEnv.createTemporaryView("T", stream, 'a, 'b, 'c)

    val sqlQuery = "SELECT a, s FROM T, UNNEST(T.c) as A (s)"

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = List("1,Hi", "1,w", "2,Hello", "2,k", "3,Hello world", "3,x")
    val results = result.toDataSet[Row].collect().toList
    assertEquals(expected.toString(), results.sortWith(_.toString < _.toString).toString())
  }

  @Test
  def testCrossWithUnnestForMap(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val data = List(
      Row.of(Int.box(1),
        Long.box(11L), {
          val map = new util.HashMap[String, String]()
          map.put("a", "10")
          map.put("b", "11")
          map
        }),
      Row.of(Int.box(2),
        Long.box(22L), {
          val map = new util.HashMap[String, String]()
          map.put("c", "20")
          map
        }),
      Row.of(Int.box(3),
        Long.box(33L), {
          val map = new util.HashMap[String, String]()
          map.put("d", "30")
          map.put("e", "31")
          map
        })
    )

    implicit val typeInfo = Types.ROW(
      Array[String]("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT,
        Types.LONG,
        Types.MAP(Types.STRING, Types.STRING))
    )
    val table = tEnv.fromDataSet(env.fromCollection(data))
    tEnv.registerTable("src", table)


    val sqlQuery =
      """
        |SELECT a,b,v FROM src
        |CROSS JOIN UNNEST(c) as f (k,v)
      """.stripMargin
    val result = tEnv.sqlQuery(sqlQuery)

    val expected = List("1,11,10", "1,11,11", "2,22,20", "3,33,30", "3,33,31")
    val results = result.collect().toList
    assertEquals(expected.toString(), results.sortWith(_.toString < _.toString).toString())
  }

  @Test
  def testJoinWithUnnestOfTuple(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val data = List(
      (1, Array((12, "45.6"), (2, "45.612"))),
      (2, Array((13, "41.6"), (1, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val stream = env.fromCollection(data)
    tEnv.createTemporaryView("T", stream, 'a, 'b)

    val sqlQuery = "" +
      "SELECT a, b, x, y " +
      "FROM " +
      "  (SELECT a, b FROM T WHERE a < 3) as tf, " +
      "  UNNEST(tf.b) as A (x, y) " +
      "WHERE x > a"

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = List(
      "1,[(12,45.6), (2,45.612)],12,45.6",
      "1,[(12,45.6), (2,45.612)],2,45.612",
      "2,[(13,41.6), (1,45.2136)],13,41.6").mkString(", ")
    val results = result.toDataSet[Row].collect().map(_.toString)
    assertEquals(expected, results.sorted.mkString(", "))
  }
}

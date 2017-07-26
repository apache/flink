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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.runtime.utils.{BatchTestData, TableProgramsCollectionTestBase}
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

@RunWith(classOf[Parameterized])
class SetOperatorsITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testUnionAll(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM t1 UNION ALL (SELECT f FROM t2)"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'd, 'e, 'f)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hi\n" + "Hello\n" + "Hello world\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUnion(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM t1 UNION (SELECT f FROM t2)"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'd, 'e, 'f)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hi\n" + "Hello\n" + "Hello world\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUnionWithFilter(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM (" +
      "SELECT * FROM t1 UNION ALL (SELECT a, b, c FROM t2))" +
      "WHERE b < 2"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'a, 'b, 'd, 'c, 'e)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hi\n" + "Hallo\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUnionWithAggregation(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT count(c) FROM (" +
      "SELECT * FROM t1 UNION ALL (SELECT a, b, c FROM t2))"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'a, 'b, 'd, 'c, 'e)

    val result = tEnv.sql(sqlQuery)

    val expected = "18"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testExcept(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM t1 EXCEPT (SELECT c FROM t2)"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = env.fromElements((1, 1L, "Hi"))
    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello\n" + "Hello world\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  @Ignore
  // calcite sql parser doesn't support EXCEPT ALL
  def testExceptAll(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM t1 EXCEPT ALL SELECT c FROM t2"

    val data1 = new mutable.MutableList[Int]
    data1 += (1, 1, 1, 2, 2)
    val data2 = new mutable.MutableList[Int]
    data2 += (1, 2, 2, 3)
    val ds1 = env.fromCollection(data1)
    val ds2 = env.fromCollection(data2)

    tEnv.registerDataSet("t1", ds1, 'c)
    tEnv.registerDataSet("t2", ds2, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "1\n1"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testExceptWithFilter(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM (" +
      "SELECT * FROM t1 EXCEPT (SELECT a, b, c FROM t2))" +
      "WHERE b < 2"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'a, 'b, 'd, 'c, 'e)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hi\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersect(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM t1 INTERSECT SELECT c FROM t2"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world!"))
    val ds2 = env.fromCollection(Random.shuffle(data))

    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hi\n" + "Hello\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  @Ignore
  // calcite sql parser doesn't support INTERSECT ALL
  def testIntersectAll(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM t1 INTERSECT ALL SELECT c FROM t2"

    val data1 = new mutable.MutableList[Int]
    data1 += (1, 1, 1, 2, 2)
    val data2 = new mutable.MutableList[Int]
    data2 += (1, 2, 2, 3)
    val ds1 = env.fromCollection(data1)
    val ds2 = env.fromCollection(data2)

    tEnv.registerDataSet("t1", ds1, 'c)
    tEnv.registerDataSet("t2", ds2, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "1\n2\n2"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersectWithFilter(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM ((SELECT * FROM t1) INTERSECT (SELECT * FROM t2)) WHERE a > 1"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get3TupleDataSet(env)

    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello\n" + "Hello world\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInWithFilter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as('d, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sql("SELECT d FROM Table5 WHERE d IN (SELECT a FROM Table3)")

    val expected = Seq("1", "2", "2", "3", "3", "3").mkString("\n")
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInWithProjection(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as('d, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sql("SELECT d IN (SELECT a FROM Table3) FROM Table5")

    val expected = Seq("false", "false", "false", "false", "false", "false", "false",
      "false", "false", "true", "true", "true", "true", "true", "true").mkString("\n")
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInWithFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("T6", ds1)

    val result = tEnv.sql("SELECT a, b, c, d, e FROM T6 WHERE a IN (c, b, 5)")

    val expected = "1,1,0,Hallo,1\n2,2,1,Hallo Welt,2\n2,3,2,Hallo Welt wie,1\n" +
      "3,4,3,Hallo Welt wie gehts?,2\n5,11,10,GHI,1\n5,12,11,HIJ,3\n5,13,12,IJK,3\n" +
      "5,14,13,JKL,2\n5,15,14,KLM,2"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInNestedTuples(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = BatchTestData.getSmall2NestedTupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T7", ds1)

    val result = tEnv.sql("SELECT a, b, c FROM T7 WHERE c IN (a)")

    val expected = "(3,3),three,(3,3)"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNotIn(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T2", ds1)

    val result = tEnv.sql(
      "SELECT a, c FROM T2 WHERE b NOT IN (SELECT b FROM T2 WHERE b = 6 OR b = 1)")

    val expected = "10,Comment#4\n11,Comment#5\n12,Comment#6\n" +
      "13,Comment#7\n14,Comment#8\n15,Comment#9\n" +
      "2,Hello\n3,Hello world\n4,Hello world, how are you?\n5,I am fine.\n" +
      "6,Luke Skywalker\n7,Comment#1\n8,Comment#2\n9,Comment#3\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  @Ignore // Calcite bug?
  def testNotInWithFilter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv).as('d, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val result = tEnv.sql("SELECT d FROM Table5 WHERE d NOT IN (SELECT a FROM Table3) AND d < 5")

    val expected = Seq("4", "4", "4", "4").mkString("\n")
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}

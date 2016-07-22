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

package org.apache.flink.api.scala.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

@RunWith(classOf[Parameterized])
class SetOperatorsITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

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

}

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

package org.apache.flink.table.runtime.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
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
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f)

    val unionDs = ds1.unionAll(ds2).select('c)

    val results = unionDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUnion(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f)

    val unionDs = ds1.union(ds2).select('c)

    val results = unionDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTernaryUnionAll(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds3 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val unionDs = ds1.unionAll(ds2).unionAll(ds3).select('c)

    val results = unionDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hello\n" + "Hello world\n" +
      "Hi\n" + "Hello\n" + "Hello world\n" +
      "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTernaryUnion(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds3 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val unionDs = ds1.union(ds2).union(ds3).select('c)

    val results = unionDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMinusAll(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromElements((1, 1L, "Hi")).toTable(tEnv, 'a, 'b, 'c)

    val minusDs = ds1.unionAll(ds1).unionAll(ds1)
      .minusAll(ds2.unionAll(ds2)).select('c)

    val results = minusDs.toDataSet[Row].collect()
    val expected = "Hi\n" +
      "Hello\n" + "Hello world\n" +
      "Hello\n" + "Hello world\n" +
      "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMinus(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromElements((1, 1L, "Hi")).toTable(tEnv, 'a, 'b, 'c)

    val minusDs = ds1.unionAll(ds1).unionAll(ds1)
      .minus(ds2.unionAll(ds2)).select('c)

    val results = minusDs.toDataSet[Row].collect()
    val expected = "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMinusDifferentFieldNames(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromElements((1, 1L, "Hi")).toTable(tEnv, 'd, 'e, 'f)

    val minusDs = ds1.unionAll(ds1).unionAll(ds1)
      .minus(ds2.unionAll(ds2)).select('c)

    val results = minusDs.toDataSet[Row].collect()
    val expected = "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersect(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world!"))
    val ds2 = env.fromCollection(Random.shuffle(data)).toTable(tEnv, 'a, 'b, 'c)

    val intersectDS = ds1.intersect(ds2).select('c).toDataSet[Row]

    val results = intersectDS.collect()

    val expected = "Hi\n" + "Hello\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersectAll(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val data1 = new mutable.MutableList[Int]
    data1 += (1, 1, 1, 2, 2)
    val data2 = new mutable.MutableList[Int]
    data2 += (1, 2, 2, 2, 3)
    val ds1 = env.fromCollection(data1).toTable(tEnv, 'c)
    val ds2 = env.fromCollection(data2).toTable(tEnv, 'c)

    val intersectDS = ds1.intersectAll(ds2).select('c).toDataSet[Row]

    val expected = "1\n2\n2"
    val results = intersectDS.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersectWithDifferentFieldNames(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'e, 'f, 'g)

    val intersectDs = ds1.intersect(ds2).select('c)

    val results = intersectDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testIntersectWithScalarExpression(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .select('a + 1, 'b, 'c)
    val ds2 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .select('a + 1, 'b, 'c)

    val intersectDs = ds1.intersect(ds2)

    val results = intersectDs.toDataSet[Row].collect()
    val expected = "2,1,Hi\n" + "3,2,Hello\n" + "4,2,Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}

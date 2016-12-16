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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala.batch.utils.SortTestUtils._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala._
import org.apache.flink.api.table.{TableEnvironment, TableException}
import org.apache.flink.types.Row
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class SortITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testOrderByMultipleFieldsWithSql(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 DESC, _2 DESC"

    implicit def rowOrdering[T <: Product] = Ordering.by((x : T) =>
      (- x.productElement(0).asInstanceOf[Int], - x.productElement(1).asInstanceOf[Long]))

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val expected = sortExpectedly(tupleDataSetStrings)
    val results = tEnv.sql(sqlQuery).toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results
      .filterNot(_.isEmpty)
      .sortBy(_.head)(Ordering.by(f=> f.toString))
      .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByWithOffset(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 DESC OFFSET 2 ROWS"

    implicit def rowOrdering[T <: Product] = Ordering.by((x : T) =>
      - x.productElement(0).asInstanceOf[Int] )

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val expected = sortExpectedly(tupleDataSetStrings, 2, 21)
    val results = tEnv.sql(sqlQuery).toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results.
      filterNot(_.isEmpty)
      .sortBy(_.head)(Ordering.by(f=> f.toString))
      .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByWithOffsetAndFetch(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY"

    implicit def rowOrdering[T <: Product] = Ordering.by((x : T) =>
      x.productElement(0).asInstanceOf[Int] )

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val expected = sortExpectedly(tupleDataSetStrings, 2, 7)
    val results = tEnv.sql(sqlQuery).toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results
      .filterNot(_.isEmpty)
      .sortBy(_.head)(Ordering.by(f=> f.toString))
      .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByLimit(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _2, _1 LIMIT 5"

    implicit def rowOrdering[T <: Product] = Ordering.by((x : T) =>
      (x.productElement(1).asInstanceOf[Long], x.productElement(0).asInstanceOf[Int]) )

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val expected = sortExpectedly(tupleDataSetStrings, 0, 5)
    val results = tEnv.sql(sqlQuery).toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results
      .filterNot(_.isEmpty)
      .sortBy(_.head)(Ordering.by(f=> f.toString))
      .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test(expected = classOf[TableException])
  def testLimitWithoutOrder(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable LIMIT 5"

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    tEnv.sql(sqlQuery).toDataSet[Row].collect()
  }

}

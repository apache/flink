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

import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.SortTestUtils._
import org.apache.flink.table.runtime.utils.TableProgramsClusterTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[Parameterized])
class SortITCase(mode: TestExecutionMode, configMode: TableConfigMode)
    extends TableProgramsClusterTestBase(mode, configMode) {

  private def getExecutionEnvironment = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // set the parallelism explicitly to make sure the query is executed in
    // a distributed manner
    env.setParallelism(3)
    env
  }

  @Test
  def testOrderByMultipleFieldsWithSql(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 DESC, _2 DESC"

    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      (- x.productElement(0).asInstanceOf[Int], - x.productElement(1).asInstanceOf[Long]))

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val expected = sortExpectedly(tupleDataSetStrings)
    // squash all rows inside a partition into one element
    val results = tEnv.sqlQuery(sqlQuery).toDataSet[Row].mapPartition(rows => {
      // the rows need to be copied in object reuse mode
      val copied = new mutable.ArrayBuffer[Row]
      rows.foreach(r => copied += Row.copy(r))
      Seq(copied)
    }).collect()

    def rowOrdering = Ordering.by((r : Row) => {
      // ordering for this tuple will fall into the previous defined tupleOrdering,
      // so we just need to return the field by their defining sequence
      (r.getField(0).asInstanceOf[Int], r.getField(1).asInstanceOf[Long])
    })

    val result = results
        .filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
        .sortBy(_.head)(rowOrdering)
        .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByWithOffset(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 DESC OFFSET 2 ROWS"

    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      - x.productElement(0).asInstanceOf[Int] )

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val expected = sortExpectedly(tupleDataSetStrings, 2, 21)
    // squash all rows inside a partition into one element
    val results = tEnv.sqlQuery(sqlQuery).toDataSet[Row].mapPartition(rows => {
      // the rows need to be copied in object reuse mode
      val copied = new mutable.ArrayBuffer[Row]
      rows.foreach(r => copied += Row.copy(r))
      Seq(copied)
    }).collect()

    val result = results.
        filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
        .sortBy(_.head)(Ordering.by((r : Row) => -r.getField(0).asInstanceOf[Int]))
        .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByWithOffsetAndFetch(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY"

    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      x.productElement(0).asInstanceOf[Int] )

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val expected = sortExpectedly(tupleDataSetStrings, 2, 7)
    // squash all rows inside a partition into one element
    val results = tEnv.sqlQuery(sqlQuery).toDataSet[Row].mapPartition(rows => {
      // the rows need to be copied in object reuse mode
      val copied = new mutable.ArrayBuffer[Row]
      rows.foreach(r => copied += Row.copy(r))
      Seq(copied)
    }).collect()

    val result = results
      .filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
      .sortBy(_.head)(Ordering.by((r : Row) => r.getField(0).asInstanceOf[Int]))
      .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByLimit(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _2, _1 LIMIT 5"

    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      (x.productElement(1).asInstanceOf[Long], x.productElement(0).asInstanceOf[Int]) )

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val expected = sortExpectedly(tupleDataSetStrings, 0, 5)
    // squash all rows inside a partition into one element
    val results = tEnv.sqlQuery(sqlQuery).toDataSet[Row].mapPartition(rows => {
      // the rows need to be copied in object reuse mode
      val copied = new mutable.ArrayBuffer[Row]
      rows.foreach(r => copied += Row.copy(r))
      Seq(copied)
    }).collect()

    def rowOrdering = Ordering.by((r : Row) => {
      // ordering for this tuple will fall into the previous defined tupleOrdering,
      // so we just need to return the field by their defining sequence
      (r.getField(0).asInstanceOf[Int], r.getField(1).asInstanceOf[Long])
    })

    val result = results
        .filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
        .sortBy(_.head)(rowOrdering)
        .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }
}

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

package org.apache.flink.table.api.scala.batch.table

import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.SortTestUtils._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsClusterTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class SortITCase(configMode: TableConfigMode) extends TableProgramsClusterTestBase(configMode) {

  private def getExecutionEnvironment = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // set the parallelism explicitly to make sure the query is executed in
    // a distributed manner
    env.setParallelism(3)
    env
  }

  @Test
  def testOrderByDesc(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).orderBy('_1.desc)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      - x.productElement(0).asInstanceOf[Int] )

    val expected = sortExpectedly(tupleDataSetStrings)
    // squash all rows inside a partition into one element
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results
        .filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
        .sortBy(_.head)(Ordering.by((r : Row) => -r.getField(0).asInstanceOf[Int]))
        .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByAsc(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).orderBy('_1.asc)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      x.productElement(0).asInstanceOf[Int] )

    val expected = sortExpectedly(tupleDataSetStrings)
    // squash all rows inside a partition into one element
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results
        .filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
        .sortBy(_.head)(Ordering.by((r : Row) => r.getField(0).asInstanceOf[Int]))
        .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByMultipleFieldsDifferentDirections(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).orderBy('_2.asc, '_1.desc)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      (x.productElement(1).asInstanceOf[Long], - x.productElement(0).asInstanceOf[Int]) )

    val expected = sortExpectedly(tupleDataSetStrings)
    // squash all rows inside a partition into one element
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

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
  def testOrderByOffset(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).orderBy('_1.asc).limit(3)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      x.productElement(0).asInstanceOf[Int] )

    val expected = sortExpectedly(tupleDataSetStrings, 3, 21)
    // squash all rows inside a partition into one element
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results
        .filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
        .sortBy(_.head)(Ordering.by((r : Row) => r.getField(0).asInstanceOf[Int]))
        .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByOffsetAndFetch(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).orderBy('_1.desc).limit(3, 5)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      - x.productElement(0).asInstanceOf[Int] )

    val expected = sortExpectedly(tupleDataSetStrings, 3, 8)
    // squash all rows inside a partition into one element
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results
        .filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
        .sortBy(_.head)(Ordering.by((r : Row) => -r.getField(0).asInstanceOf[Int]))
        .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByFetch(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).orderBy('_1.asc).limit(0, 5)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      x.productElement(0).asInstanceOf[Int] )

    val expected = sortExpectedly(tupleDataSetStrings, 0, 5)
    // squash all rows inside a partition into one element
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    implicit def rowOrdering = Ordering.by((r : Row) => r.getField(0).asInstanceOf[Int])

    val result = results
        .filterNot(_.isEmpty)
        // sort all partitions by their head element to verify the order across partitions
        .sortBy(_.head)
        .reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

}

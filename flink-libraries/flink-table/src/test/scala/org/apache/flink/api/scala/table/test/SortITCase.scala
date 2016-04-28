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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import org.apache.flink.api.table.test.utils.TableProgramsTestBase.TableConfigMode
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

  def getExecutionEnvironment = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env
  }

  val tupleDataSetStrings = List((1, 1L, "Hi")
    ,(2, 2L, "Hello")
    ,(3, 2L, "Hello world")
    ,(4, 3L, "Hello world, how are you?")
    ,(5, 3L, "I am fine.")
    ,(6, 3L, "Luke Skywalker")
    ,(7, 4L, "Comment#1")
    ,(8, 4L, "Comment#2")
    ,(9, 4L, "Comment#3")
    ,(10, 4L, "Comment#4")
    ,(11, 5L, "Comment#5")
    ,(12, 5L, "Comment#6")
    ,(13, 5L, "Comment#7")
    ,(14, 5L, "Comment#8")
    ,(15, 5L, "Comment#9")
    ,(16, 6L, "Comment#10")
    ,(17, 6L, "Comment#11")
    ,(18, 6L, "Comment#12")
    ,(19, 6L, "Comment#13")
    ,(20, 6L, "Comment#14")
    ,(21, 6L, "Comment#15"))

  @Test
  def testOrderByDesc(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).orderBy('_1.desc)
    implicit def rowOrdering[T <: Product] = Ordering.by((x : T) =>
      - x.productElement(0).asInstanceOf[Int])

    val expected = sortExpectedly(tupleDataSetStrings)
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results.filterNot(_.isEmpty).sortBy(p => p.head).reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByAsc(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).orderBy('_1.asc)
    implicit def rowOrdering[T <: Product] = Ordering.by((x : T) =>
      x.productElement(0).asInstanceOf[Int])

    val expected = sortExpectedly(tupleDataSetStrings)
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results.filterNot(_.isEmpty).sortBy(p => p.head).reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByMultipleFieldsDifferentDirections(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    val t = ds.toTable(tEnv).orderBy('_1.asc, '_2.desc)
    implicit def rowOrdering[T <: Product] = Ordering.by((x : T) =>
      (x.productElement(0).asInstanceOf[Int], - x.productElement(1).asInstanceOf[Long]))

    val expected = sortExpectedly(tupleDataSetStrings)
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results.filterNot(_.isEmpty).sortBy(p => p.head).reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByMultipleFieldsWithSql(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 DESC, _2 DESC"
    implicit def rowOrdering[T <: Product] = Ordering.by((x : T) =>
      (- x.productElement(0).asInstanceOf[Int], - x.productElement(1).asInstanceOf[Long]))

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val expected = sortExpectedly(tupleDataSetStrings)
    val results = tEnv.sql(sqlQuery).toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    val result = results.filterNot(_.isEmpty).sortBy(p => p.head).reduceLeft(_ ++ _)

    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  def sortExpectedly(dataSet: List[Product])(implicit ordering: Ordering[Product]): String = {
    dataSet.sorted(ordering).mkString("\n").replaceAll("[\\(\\)]", "")
  }

}

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

package org.apache.flink.table.planner.runtime.batch.table

import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.SortTestUtils.{sortExpectedly, tupleDataSetStrings}
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, CollectionBatchExecTable}
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.test.util.TestBaseUtils

import org.junit._

import scala.collection.JavaConverters._

class SortITCase extends BatchTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  def compare(t: Table, expected: String): Unit = {
    TestBaseUtils.compareOrderedResultAsText(executeQuery(t).asJava, expected)
  }

  @Test
  def testOrderByDesc(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv)
    val t = ds.orderBy('_1.desc)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      -x.productElement(0).asInstanceOf[Int])
    compare(t, sortExpectedly(tupleDataSetStrings))
  }

  @Test
  def testOrderByAsc(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv)
    val t = ds.orderBy('_1.asc)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      x.productElement(0).asInstanceOf[Int])
    compare(t, sortExpectedly(tupleDataSetStrings))
  }

  @Test
  def testOrderByMultipleFieldsDifferentDirections(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv)
    val t = ds.orderBy('_2.asc, '_1.desc)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      (x.productElement(1).asInstanceOf[Long], - x.productElement(0).asInstanceOf[Int]) )
    compare(t, sortExpectedly(tupleDataSetStrings))
  }

  @Ignore //TODO something not support?
  @Test
  def testOrderByOffset(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv)
    val t = ds.orderBy('_1.asc).offset(3)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      x.productElement(0).asInstanceOf[Int] )
    compare(t, sortExpectedly(tupleDataSetStrings, 3, 21))
  }

  @Test
  def testOrderByOffsetAndFetch(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv)
    val t = ds.orderBy('_1.desc).offset(3).fetch(5)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      - x.productElement(0).asInstanceOf[Int] )
    compare(t, sortExpectedly(tupleDataSetStrings, 3, 8))
  }

  @Test
  def testOrderByFetch(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv)
    val t = ds.orderBy('_1.asc).offset(0).fetch(5)
    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
      x.productElement(0).asInstanceOf[Int] )
    compare(t, sortExpectedly(tupleDataSetStrings, 0, 5))
  }
}

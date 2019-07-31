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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.types.Row

import org.junit._

class SortLimitITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    registerCollection("Table3", data3, type3, "a, b, c")
  }

  @Test
  def testOrderByWithOffsetAndFetch(): Unit = {
    testOrderByWithOffsetAndFetch(true)
    testOrderByWithOffsetAndFetch(false)
  }

  private def testOrderByWithOffsetAndFetch(order: Boolean): Unit = {

    val sqlOrder = if (order) "ASC" else "DESC"

    val expected = data3.sortBy((x : Row) =>
      if (order) x.getField(0).asInstanceOf[Int]
      else -x.getField(0).asInstanceOf[Int]).slice(2, 7)

    checkResult(
      s"SELECT * FROM Table3 ORDER BY a $sqlOrder OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY",
      expected,
      isSorted = true)
  }

  @Test
  def testOrderByLimit(): Unit = {
    testOrderByLimit(order1 = true, order2 = false)
    testOrderByLimit(order1 = false, order2 = false)
    testOrderByLimit(order1 = true, order2 = true)
    testOrderByLimit(order1 = false, order2 = true)
  }

  private def testOrderByLimit(order1: Boolean, order2: Boolean): Unit = {

    val sqlOrder1 = if (order1) "ASC" else "DESC"
    val sqlOrder2 = if (order2) "ASC" else "DESC"

    val expected = data3.sortBy((x : Row) => (
        if (order2) {
          x.getField(1).asInstanceOf[Long]
        } else {
          -x.getField(1).asInstanceOf[Long]
        },
        if (order1) {
          x.getField(0).asInstanceOf[Int]
        } else {
          -x.getField(0).asInstanceOf[Int]
        })).slice(0, 5)

    checkResult(
      s"SELECT * FROM Table3 ORDER BY b $sqlOrder2, a $sqlOrder1 LIMIT 5",
      expected,
      isSorted = true)
  }

  @Test
  def testOrderByLessThanOffset(): Unit = {

    val expected = data3.sortBy((x : Row) =>
      x.getField(0).asInstanceOf[Int]).slice(2, data3.size)

    checkResult(
      s"SELECT * FROM Table3 ORDER BY a ASC OFFSET 2 ROWS FETCH NEXT 50 ROWS ONLY",
      expected,
      isSorted = true)
  }

  @Test
  def testOrderByLimitBehindField(): Unit = {
    val expected = data3.sortBy((x : Row) => x.getField(2).asInstanceOf[String]).slice(0, 5)

    checkResult(
      s"SELECT * FROM Table3 ORDER BY c LIMIT 5",
      expected,
      isSorted = true)
  }

  @Test
  def testOrderBehindField(): Unit = {
    conf.getConfiguration.setInteger(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)
    val expected = data3.sortBy((x : Row) => x.getField(2).asInstanceOf[String])

    checkResult(
      s"SELECT * FROM Table3 ORDER BY c",
      expected,
      isSorted = true)
  }

  @Test
  def testOrderByRepeatedFields(): Unit = {
    val expected = data3.sortBy((x : Row) => x.getField(0).asInstanceOf[Int]).slice(0, 5)
    checkResult(
      s"SELECT * FROM Table3 ORDER BY a, a, a LIMIT 5",
      expected,
      isSorted = true)
  }
}

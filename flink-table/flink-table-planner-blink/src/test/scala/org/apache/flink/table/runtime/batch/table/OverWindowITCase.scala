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

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Over
import org.apache.flink.table.runtime.utils.BatchTestBase
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.utils.TestData._

import org.junit.{Before, Test}

class OverWindowITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    registerCollection(
      "Table1", data1, type1, "month, area, product", nullablesOfData1)
  }

  @Test
  def testSingleRowOverWindow(): Unit = {
    val table = tEnv.scan("Table1")

    val expected = Seq(
      row("a", 1, 5),
      row("a", 2, 6),
      row("b", 3, 7),
      row("b", 4, 8),
      row("c", 5, 9),
      row("c", 6, 10)
    )
    checkTableResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding 0.rows following 0.rows as 'w)
        .select('area, 'month, 'product.sum over 'w),
      expected
    )

    checkTableResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding CURRENT_ROW following 0.rows as 'w)
        .select('area, 'month, 'product.sum over 'w),
      expected
    )

    checkTableResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding 0.rows following CURRENT_ROW as 'w)
        .select('area, 'month, 'product.sum over 'w),
      expected
    )

    checkTableResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding CURRENT_ROW following CURRENT_ROW as 'w)
        .select('area, 'month, 'product.sum over 'w),
      expected
    )
  }
}

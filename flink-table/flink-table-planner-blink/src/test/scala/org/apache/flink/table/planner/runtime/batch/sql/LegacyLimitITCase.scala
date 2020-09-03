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

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.utils.TestLegacyLimitableTableSource

import org.junit._

class LegacyLimitITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)

    TestLegacyLimitableTableSource.createTemporaryTable(
      tEnv, data3, new TableSchema(Array("a", "b", "c"), type3.getFieldTypes), "LimitTable")
  }

  @Test
  def testOffsetAndFetch(): Unit = {
    checkSize(
      "SELECT * FROM Table3 OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY",
      5)
  }

  @Test
  def testOffsetAndLimit(): Unit = {
    checkSize(
      "SELECT * FROM Table3 LIMIT 10 OFFSET 2",
      10)
  }

  @Test
  def testFetch(): Unit = {
    checkSize(
      "SELECT * FROM Table3 FETCH NEXT 10 ROWS ONLY",
      10)
  }

  @Test
  def testFetchWithLimitTable(): Unit = {
    checkSize(
      "SELECT * FROM LimitTable FETCH NEXT 10 ROWS ONLY",
      10)
  }

  @Test
  def testFetchFirst(): Unit = {
    checkSize(
      "SELECT * FROM Table3 FETCH FIRST 10 ROWS ONLY",
      10)
  }

  @Test
  def testFetchFirstWithLimitTable(): Unit = {
    checkSize(
      "SELECT * FROM LimitTable FETCH FIRST 10 ROWS ONLY",
      10)
  }

  @Test
  def testLimit(): Unit = {
    checkSize(
      "SELECT * FROM Table3 LIMIT 5",
      5)
  }

  @Test
  def testLimit0WithLimitTable(): Unit = {
    checkSize(
      "SELECT * FROM LimitTable LIMIT 0",
      0)
  }

  @Test
  def testLimitWithLimitTable(): Unit = {
    checkSize(
      "SELECT * FROM LimitTable LIMIT 5",
      5)
  }

  @Test
  def testLessThanOffset(): Unit = {
    checkSize(
      "SELECT * FROM Table3 OFFSET 2 ROWS FETCH NEXT 50 ROWS ONLY",
      19)
  }

  @Test
  def testLessThanOffsetWithLimitSource(): Unit = {
    checkSize(
      "SELECT * FROM LimitTable OFFSET 2 ROWS FETCH NEXT 50 ROWS ONLY",
      19)
  }
}

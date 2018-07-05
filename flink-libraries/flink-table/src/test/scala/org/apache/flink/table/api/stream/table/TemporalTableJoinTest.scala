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
package org.apache.flink.table.api.stream.table

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableSchema, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.ResolvedFieldReference
import org.apache.flink.table.functions.TemporalTableFunction
import org.apache.flink.table.utils._
import org.junit.Assert.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.Test

class TemporalTableJoinTest extends TableTestBase {

  val util: TableTestUtil = streamTestUtil()

  val ratesHistory = util.addTable[(String, Int, Timestamp)](
    "RatesHistory", 'currency, 'rate, 'rowtime.rowtime)

  val rates = util.addFunction(
    "Rates",
    ratesHistory.createTemporalTableFunction('rowtime, 'currency))

  @Test
  def testTemporalTableFunctionScan(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Cannot translate a query with an unbounded table function call")

    val result = rates(java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123"))
    util.printTable(result)
  }

  @Test
  def testProcessingTimeIndicatorVersion(): Unit = {
    val util: TableTestUtil = streamTestUtil()
    val ratesHistory = util.addTable[(String, Int)](
      "RatesHistory", 'currency, 'rate, 'proctime.proctime)
    val rates = ratesHistory.createTemporalTableFunction('proctime, 'currency)
    assertRatesFunction(ratesHistory.getSchema, rates, true)
  }

  @Test
  def testValidStringFieldReference(): Unit = {
    val rates = ratesHistory.createTemporalTableFunction("rowtime", "currency")
    assertRatesFunction(ratesHistory.getSchema, rates)
  }

  private def assertRatesFunction(
      expectedSchema: TableSchema,
      rates: TemporalTableFunction,
      proctime: Boolean = false): Unit = {
    assertEquals("currency", rates.getPrimaryKey)
    assertTrue(rates.getTimeAttribute.isInstanceOf[ResolvedFieldReference])
    assertEquals(
      if (proctime) "proctime" else "rowtime",
      rates.getTimeAttribute.asInstanceOf[ResolvedFieldReference].name)
    assertArrayEquals(
      expectedSchema.getColumnNames.asInstanceOf[Array[Object]],
      rates.getResultType.getFieldNames.asInstanceOf[Array[Object]])
    assertArrayEquals(
      expectedSchema.getTypes.asInstanceOf[Array[Object]],
      rates.getResultType.getFieldTypes.asInstanceOf[Array[Object]])
  }
}


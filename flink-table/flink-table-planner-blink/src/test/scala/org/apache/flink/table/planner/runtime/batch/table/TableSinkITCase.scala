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
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.util.ExceptionUtils

import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

import scala.collection.JavaConversions._

class TableSinkITCase extends BatchTestBase {

  @Test
  def testDecimalOnOutputFormatTableSink(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE sink (
         |  `c` VARCHAR(5),
         |  `b` DECIMAL(10, 0),
         |  `d` CHAR(5)
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true',
         |  'runtime-sink' = 'OutputFormat'
         |)
         |""".stripMargin)

    registerCollection("MyTable", data3, type3, "a, b, c", nullablesOfData3)

    val table = tEnv.from("MyTable")
      .where('a > 20)
      .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
    table.executeInsert("sink").await()

    val result = TestValuesTableFactory.getResults("sink")
    val expected = Seq("12345,55,12345")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testDecimalOnSinkFunctionTableSink(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE sink (
         |  `c` VARCHAR(5),
         |  `b` DECIMAL(10, 0),
         |  `d` CHAR(5)
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", data3, type3, "a, b, c", nullablesOfData3)

    val table = tEnv.from("MyTable")
      .where('a > 20)
      .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
    table.executeInsert("sink").await()

    val result = TestValuesTableFactory.getResults("sink")
    val expected = Seq("12345,55,12345")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testSinkWithKey(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` DOUBLE,
         |  PRIMARY KEY (a) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "a, b", nullableOfSimpleData2)

    val table = tEnv.from("MyTable")
      .groupBy('a)
      .select('a, 'b.sum())
    table.executeInsert("testSink").await()

    val result = TestValuesTableFactory.getResults("testSink")
    val expected = List(
      "1,0.1",
      "2,0.4",
      "3,1.0",
      "4,2.2",
      "5,3.9")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testSinkWithoutKey(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` DOUBLE
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "a, b", nullableOfSimpleData2)

    val table = tEnv.from("MyTable")
      .groupBy('a)
      .select('a, 'b.sum())
    table.executeInsert("testSink").await()

    val result = TestValuesTableFactory.getResults("testSink")
    val expected = List(
      "1,0.1",
      "2,0.4",
      "3,1.0",
      "4,2.2",
      "5,3.9")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testNotNullEnforcer(): Unit = {
    val dataId = TestValuesTableFactory.registerData(nullData4)
    tEnv.executeSql(
      s"""
         |CREATE TABLE nullable_src (
         |  category STRING,
         |  shopId INT,
         |  num INT
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin)
    tEnv.executeSql(
      s"""
         |CREATE TABLE not_null_sink (
         |  category STRING,
         |  shopId INT,
         |  num INT NOT NULL
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    // default should fail, because there are null values in the source
    try {
      tEnv.executeSql("INSERT INTO not_null_sink SELECT * FROM nullable_src").await()
      fail("Execution should fail.")
    } catch {
      case t: Throwable =>
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          "Column 'num' is NOT NULL, however, a null value is being written into it. " +
            "You can set job configuration 'table.exec.sink.not-null-enforcer'='drop' " +
            "to suppress this exception and drop such records silently.")
        assertTrue(exception.isPresent)
    }

    // enable drop enforcer to make the query can run
    tEnv.getConfig.getConfiguration.setString("table.exec.sink.not-null-enforcer", "drop")
    tEnv.executeSql("INSERT INTO not_null_sink SELECT * FROM nullable_src").await()

    val result = TestValuesTableFactory.getResults("not_null_sink")
    val expected = List("book,1,12", "book,4,11", "fruit,3,44")
    assertEquals(expected.sorted, result.sorted)
  }
}

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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.table.api._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.util.ExceptionUtils
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

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
    innerTestNotNullEnforcer("SinkFunction")
  }

  @Test
  def testDataStreamNotNullEnforcer(): Unit = {
    innerTestNotNullEnforcer("DataStream")
  }

  @Test
  def testParallelismWithDataStream(): Unit = {
    Try(innerTestSetParallelism("DataStreamWithParallelism", 1, index = 1)) match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          "`DataStreamSinkProvider` is not allowed to work with `ParallelismProvider`, please see document of `ParallelismProvider`")
        assertTrue(exception.isPresent)
      }
    }
  }

  @Test
  def testParallelismWithSinkFunction(): Unit = {
    val taskParallelism = env.getParallelism
    val oversizeParallelism = taskParallelism + 1
    val negativeParallelism = -1
    val vaildParallelism = 1
    val index = new AtomicInteger(1)

    Try(innerTestSetParallelism("SinkFunction", oversizeParallelism, index = index.getAndIncrement)) match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          s"the configured sink parallelism: ${oversizeParallelism} is larger than the task max parallelism: ${taskParallelism}")
        assertTrue(exception.isPresent)
      }
    }

    Try(innerTestSetParallelism("SinkFunction", negativeParallelism, index = index.getAndIncrement)) match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          s"the configured sink parallelism: ${negativeParallelism} should not be little or equal zero")
        assertTrue(exception.isPresent)
      }
    }

    assertTrue(Try(innerTestSetParallelism("SinkFunction", vaildParallelism, index = index.getAndIncrement)).isSuccess)
  }

  @Test
  def testParallelismWithOutputFormat(): Unit = {
    val taskParallelism = env.getParallelism
    val oversizeParallelism = taskParallelism + 1
    val negativeParallelism = -1
    val vaildParallelism = 1
    val index = new AtomicInteger(1)

    Try(innerTestSetParallelism("OutputFormat", oversizeParallelism, index = index.getAndIncrement)) match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          s"the configured sink parallelism: ${oversizeParallelism} is larger than the task max parallelism: ${taskParallelism}")
        assertTrue(exception.isPresent)
      }
    }

    Try(innerTestSetParallelism("OutputFormat", negativeParallelism, index = index.getAndIncrement)) match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          s"the configured sink parallelism: ${negativeParallelism} should not be less than zero or equal to zero")
        assertTrue(exception.isPresent)
      }
    }

    assertTrue(Try(innerTestSetParallelism("SinkFunction", vaildParallelism, index = index.getAndIncrement)).isSuccess)
  }


  def innerTestSetParallelism(provider: String, parallelism: Int, index: Int): Unit = {
    val dataId = TestValuesTableFactory.registerData(data1)
    val sourceTableName = s"test_para_source_${provider.toLowerCase.trim}_$index"
    val sinkTableName = s"test_para_sink_${provider.toLowerCase.trim}_$index"
    tEnv.executeSql(
      s"""
         |CREATE TABLE $sourceTableName (
         |  the_month INT,
         |  area STRING,
         |  product INT
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin)
    tEnv.executeSql(
      s"""
         |CREATE TABLE $sinkTableName (
         |  the_month INT,
         |  area STRING,
         |  product INT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true',
         |  'runtime-sink' = '$provider',
         |  'sink.parallelism' = '$parallelism'
         |)
         |""".stripMargin)
    tEnv.executeSql(s"INSERT INTO $sinkTableName SELECT * FROM $sourceTableName").await()
  }

  def innerTestNotNullEnforcer(provider: String): Unit = {
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
         |  'sink-insert-only' = 'true',
         |  'runtime-sink' = '$provider'
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

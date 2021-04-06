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
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.util.ExceptionUtils
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}

import scala.collection.JavaConversions._

class TableSinkITCase extends BatchTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  var _expectedEx: ExpectedException = ExpectedException.none

  @Rule
  def expectedEx: ExpectedException = _expectedEx

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

  @Test
  def testSinkWithPartitionAndComputedColumn(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` AS `a` + 1,
         |  `c` STRING,
         |  `d` INT,
         |  `e` DOUBLE
         |)
         |PARTITIONED BY (`c`, `d`)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink PARTITION(`c`='2021', `d`=1)
         |SELECT x, sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
    val expected = List(
      "1,2021,1,0.1",
      "2,2021,1,0.4",
      "3,2021,1,1.0",
      "4,2021,1,2.2",
      "5,2021,1,3.9")
    val result = TestValuesTableFactory.getResults("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsert(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` DOUBLE
         |)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink (b)
         |SELECT sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
    val expected = List(
      "null,0.1",
      "null,0.4",
      "null,1.0",
      "null,2.2",
      "null,3.9")
    val result = TestValuesTableFactory.getResults("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithNotNullColumn(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT NOT NULL,
         |  `b` DOUBLE
         |)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage("Column 'a' has no default value and does not allow NULLs")

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink (b)
         |SELECT sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
  }

  @Test
  def testPartialInsertWithPartitionAndComputedColumn(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` AS `a` + 1,
         |  `c` STRING,
         |  `d` INT,
         |  `e` DOUBLE
         |)
         |PARTITIONED BY (`c`, `d`)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink PARTITION(`c`='2021', `d`=1) (e)
         |SELECT sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
    val expected = List(
      "null,2021,1,0.1",
      "null,2021,1,0.4",
      "null,2021,1,1.0",
      "null,2021,1,2.2",
      "null,2021,1,3.9")
    val result = TestValuesTableFactory.getResults("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testFullInsertWithPartitionAndComputedColumn(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` AS `a` + 1,
         |  `c` STRING,
         |  `d` INT,
         |  `e` DOUBLE
         |)
         |PARTITIONED BY (`c`, `d`)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink PARTITION(`c`='2021', `d`=1) (a, e)
         |SELECT x, sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
    val expected = List(
      "1,2021,1,0.1",
      "2,2021,1,0.4",
      "3,2021,1,1.0",
      "4,2021,1,2.2",
      "5,2021,1,3.9")
    val result = TestValuesTableFactory.getResults("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicPartitionAndComputedColumn1(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` AS `a` + 1,
         |  `c` STRING,
         |  `d` INT,
         |  `e` DOUBLE
         |)
         |PARTITIONED BY (`c`, `d`)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink (e)
         |SELECT sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
    val expected = List(
      "null,null,null,0.1",
      "null,null,null,0.4",
      "null,null,null,1.0",
      "null,null,null,2.2",
      "null,null,null,3.9")
    val result = TestValuesTableFactory.getResults("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicPartitionAndComputedColumn2(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` AS `a` + 1,
         |  `c` STRING,
         |  `d` INT,
         |  `e` DOUBLE
         |)
         |PARTITIONED BY (`c`, `d`)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink (c, d, e)
         |SELECT '2021', 1, sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
    val expected = List(
      "null,2021,1,0.1",
      "null,2021,1,0.4",
      "null,2021,1,1.0",
      "null,2021,1,2.2",
      "null,2021,1,3.9")
    val result = TestValuesTableFactory.getResults("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithReorder(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` AS `a` + 1,
         |  `c` STRING,
         |  `d` INT,
         |  `e` DOUBLE
         |)
         |PARTITIONED BY (`c`, `d`)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink (e, d, c)
         |SELECT sum(y), 1, '2021' FROM MyTable GROUP BY x
         |""".stripMargin).await()
    val expected = List(
      "null,2021,1,0.1",
      "null,2021,1,0.4",
      "null,2021,1,1.0",
      "null,2021,1,2.2",
      "null,2021,1,3.9")
    val result = TestValuesTableFactory.getResults("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicAndStaticPartition1(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` AS `a` + 1,
         |  `c` STRING,
         |  `d` INT,
         |  `e` DOUBLE
         |)
         |PARTITIONED BY (`c`, `d`)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink PARTITION(`c`='2021') (d, e)
         |SELECT 1, sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
    val expected = List(
      "null,2021,1,0.1",
      "null,2021,1,0.4",
      "null,2021,1,1.0",
      "null,2021,1,2.2",
      "null,2021,1,3.9")
    val result = TestValuesTableFactory.getResults("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicAndStaticPartition2(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` AS `a` + 1,
         |  `c` STRING,
         |  `d` INT,
         |  `e` DOUBLE
         |)
         |PARTITIONED BY (`c`, `d`)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink PARTITION(`c`='2021') (e)
         |SELECT sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
    val expected = List(
      "null,2021,null,0.1",
      "null,2021,null,0.4",
      "null,2021,null,1.0",
      "null,2021,null,2.2",
      "null,2021,null,3.9")
    val result = TestValuesTableFactory.getResults("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicAndStaticPartition3(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE testSink (
         |  `a` INT,
         |  `b` AS `a` + 1,
         |  `c` STRING,
         |  `d` INT,
         |  `e` DOUBLE
         |)
         |PARTITIONED BY (`c`, `d`)
         |WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    registerCollection("MyTable", simpleData2, simpleType2, "x, y", nullableOfSimpleData2)

    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage("Target column 'e' is assigned more than once")

    tEnv.executeSql(
      s"""
         |INSERT INTO testSink PARTITION(`c`='2021') (e, e)
         |SELECT 1, sum(y) FROM MyTable GROUP BY x
         |""".stripMargin).await()
  }
}

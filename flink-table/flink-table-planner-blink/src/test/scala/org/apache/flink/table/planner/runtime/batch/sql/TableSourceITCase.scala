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

import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, TestData}
import org.apache.flink.table.planner.utils._
import org.apache.flink.types.Row

import org.junit.{Before, Test}

import java.lang.{Boolean => JBool, Integer => JInt, Long => JLong}

class TableSourceITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    val myTableDataId = TestValuesTableFactory.registerData(TestData.smallData3)
    tEnv.executeSql(
      s"""
        |CREATE TABLE MyTable (
        |  `a` INT,
        |  `b` BIGINT,
        |  `c` STRING
        |) WITH (
        |  'connector' = 'values',
        |  'data-id' = '$myTableDataId',
        |  'bounded' = 'true'
        |)
        |""".stripMargin)

    val filterableTableDataId = TestValuesTableFactory.registerData(
      TestFilterableTableSource.defaultRows)
    // TODO: [FLINK-17425] support filter pushdown for TestValuesTableSource
    tEnv.executeSql(
      s"""
         |CREATE TABLE FilterableTable (
         |  name STRING,
         |  id BIGINT,
         |  amount INT,
         |  price DOUBLE
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$filterableTableDataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin)
  }

  @Test
  def testSimpleProject(): Unit = {
    checkResult(
      "SELECT a, c FROM MyTable",
      Seq(
        row(1, "Hi"),
        row(2, "Hello"),
        row(3, "Hello world"))
    )
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    checkResult(
      "SELECT COUNT(*) FROM MyTable",
      Seq(row(3))
    )
  }

  @Test
  def testNestedProject(): Unit = {
    val data = Seq(
      Row.of(new JLong(1),
        Row.of(
          Row.of("Sarah", new JInt(100)),
          Row.of(new JInt(1000), new JBool(true))
        ),
        Row.of("Peter", new JInt(10000)),
        "Mary"),
      Row.of(new JLong(2),
        Row.of(
          Row.of("Rob", new JInt(200)),
          Row.of(new JInt(2000), new JBool(false))
        ),
        Row.of("Lucy", new JInt(20000)),
        "Bob"),
      Row.of(new JLong(3),
        Row.of(
          Row.of("Mike", new JInt(300)),
          Row.of(new JInt(3000), new JBool(true))
        ),
        Row.of("Betty", new JInt(30000)),
        "Liz"))

    val dataId = TestValuesTableFactory.registerData(data)

    // TODO: [FLINK-17428] support nested project for TestValuesTableSource
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id BIGINT,
         |  deepNested ROW<
         |     nested1 ROW<name STRING, `value` INT>,
         |     nested2 ROW<num INT, flag BOOLEAN>
         |   >,
         |   nested ROW<name STRING, `value` INT>,
         |   name STRING,
         |   lower_name AS LOWER(name)
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl)

    checkResult(
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum,
        |    lower_name
        |FROM T
      """.stripMargin,
      Seq(row(1, "Sarah", 10000, true, 1000, "mary"),
        row(2, "Rob", 20000, false, 2000, "bob"),
        row(3, "Mike", 30000, true, 3000, "liz")
      )
    )
  }

  @Test
  def testTableSourceWithFilterable(): Unit = {
    checkResult(
      "SELECT id, name FROM FilterableTable WHERE amount > 4 AND price < 9",
      Seq(
        row(5, "Record_5"),
        row(6, "Record_6"),
        row(7, "Record_7"),
        row(8, "Record_8"))
    )
  }

  @Test
  def testTableSourceWithFunctionFilterable(): Unit = {
    checkResult(
      "SELECT id, name FROM FilterableTable " +
        "WHERE amount > 4 AND price < 9 AND upper(name) = 'RECORD_5'",
      Seq(
        row(5, "Record_5"))
    )
  }

  @Test
  def testInputFormatSource(): Unit = {
    val dataId = TestValuesTableFactory.registerData(TestData.smallData3)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MyInputFormatTable (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true',
         |  'runtime-source' = 'InputFormat'
         |)
         |""".stripMargin
    )

    checkResult(
      "SELECT a, c FROM MyInputFormatTable",
      Seq(
        row(1, "Hi"),
        row(2, "Hello"),
        row(3, "Hello world"))
    )
  }

  @Test
  def testAllDataTypes(): Unit = {
    val dataId = TestValuesTableFactory.registerData(TestData.fullDataTypesData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE T (
         |  `a` BOOLEAN,
         |  `b` TINYINT,
         |  `c` SMALLINT,
         |  `d` INT,
         |  `e` BIGINT,
         |  `f` FLOAT,
         |  `g` DOUBLE,
         |  `h` DECIMAL(5, 2),
         |  `i` VARCHAR(5),
         |  `j` CHAR(5),
         |  `k` DATE,
         |  `l` TIME(0),
         |  `m` TIMESTAMP(9),
         |  `n` TIMESTAMP(9) WITH LOCAL TIME ZONE,
         |  `o` ARRAY<BIGINT>,
         |  `p` ROW<f1 BIGINT, f2 STRING, f3 DOUBLE>,
         |  `q` MAP<STRING, INT>
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    )

    checkResult(
      "SELECT * FROM T",
      Seq(
        row(
          true, 127, 32767, 2147483647, 9223372036854775807L, "-1.123", "-1.123", "5.10",
          1, 1, "1969-01-01", "00:00:00.123", "1969-01-01T00:00:00.123456789",
          "1969-01-01T00:00:00.123456789Z", "[1, 2, 3]", row(1, "a", "2.3"), "{k1=1}"),
        row(
          false, -128, -32768, -2147483648, -9223372036854775808L, "3.4", "3.4", "6.10",
          12, 12, "1970-09-30", "01:01:01.123", "1970-09-30T01:01:01.123456",
          "1970-09-30T01:01:01.123456Z", "[4, 5]", row(null, "b", "4.56"), "{k4=4, k2=2}"),
        row(
          true, 0, 0, 0, 0, "0.12", "0.12", "7.10",
          123, 123, "1990-12-24", "08:10:24.123", "1990-12-24T08:10:24.123",
          "1990-12-24T08:10:24.123Z", "[6, null, 7]", row(3, null, "7.86"), "{k3=null}"),
        row(
          false, 5, 4, 123, 1234, "1.2345", "1.2345", "8.12",
          1234, 1234, "2020-05-01", "23:23:23", "2020-05-01T23:23:23",
          "2020-05-01T23:23:23Z", "[8]", row(4, "c", null), "{null=3}"),
        row(
          null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null)
      )
    )
  }
}

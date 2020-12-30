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
package org.apache.flink.table.planner.runtime.batch.sql.join

import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, InMemoryLookupableTableSource}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Assume, Before, Test}

import java.lang.{Boolean => JBoolean}
import java.util

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class LookupJoinITCase(legacyTableSource: Boolean, isAsyncMode: Boolean) extends BatchTestBase {

  val data = List(
    rowOf(1L, 12L, "Julian"),
    rowOf(2L, 15L, "Hello"),
    rowOf(3L, 15L, "Fabian"),
    rowOf(8L, 11L, "Hello world"),
    rowOf(9L, 12L, "Hello world!"))

  val dataWithNull = List(
    rowOf(null, 15L, "Hello"),
    rowOf(3L, 15L, "Fabian"),
    rowOf(null, 11L, "Hello world"),
    rowOf(9L, 12L, "Hello world!"))

  val userData = List(
    rowOf(11, 1L, "Julian"),
    rowOf(22, 2L, "Jark"),
    rowOf(33, 3L, "Fabian"))

  val userDataWithNull = List(
    rowOf(11, 1L, "Julian"),
    rowOf(22, null, "Hello"),
    rowOf(33, 3L, "Fabian"),
    rowOf(44, null, "Hello world"))

  @Before
  override def before() {
    super.before()
    createScanTable("T", data)
    createScanTable("nullableT", dataWithNull)

    createLookupTable("userTable", userData)
    createLookupTable("userTableWithNull", userDataWithNull)
    createLookupTableWithComputedColumn("userTableWithComputedColumn", userData)

    // TODO: enable object reuse until [FLINK-12351] is fixed.
    env.getConfig.disableObjectReuse()
  }

  @After
  override def after(): Unit = {
    if (legacyTableSource) {
      assertEquals(0, InMemoryLookupableTableSource.RESOURCE_COUNTER.get())
    } else {
      assertEquals(0, TestValuesTableFactory.RESOURCE_COUNTER.get())
    }
  }

  private def createLookupTable(tableName: String, data: List[Row]): Unit = {
    if (legacyTableSource) {
      val userSchema = TableSchema.builder()
        .field("age", Types.INT)
        .field("id", Types.LONG)
        .field("name", Types.STRING)
        .build()
      InMemoryLookupableTableSource.createTemporaryTable(
        tEnv, isAsyncMode, data, userSchema, tableName, isBounded = true)
    } else {
      val dataId = TestValuesTableFactory.registerData(data)
      tEnv.executeSql(
        s"""
           |CREATE TABLE $tableName (
           |  `age` INT,
           |  `id` BIGINT,
           |  `name` STRING
           |) WITH (
           |  'connector' = 'values',
           |  'data-id' = '$dataId',
           |  'async' = '$isAsyncMode',
           |  'bounded' = 'true'
           |)
           |""".stripMargin)
    }
  }

  private def createLookupTableWithComputedColumn(tableName: String, data: List[Row]): Unit = {
    if (!legacyTableSource) {
      val dataId = TestValuesTableFactory.registerData(data)
      tEnv.executeSql(
        s"""
           |CREATE TABLE $tableName (
           |  `age` INT,
           |  `id` BIGINT,
           |  `name` STRING,
           |  `nominal_age` as age + 1
           |) WITH (
           |  'connector' = 'values',
           |  'data-id' = '$dataId',
           |  'async' = '$isAsyncMode',
           |  'bounded' = 'true'
           |)
           |""".stripMargin)
    }
  }

  private def createScanTable(tableName: String, data: List[Row]): Unit = {
    val dataId = TestValuesTableFactory.registerData(data)
    tEnv.executeSql(
      s"""
         |CREATE TABLE $tableName (
         |  `id` BIGINT,
         |  `len` BIGINT,
         |  `content` STRING,
         |  `proctime` AS PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin)
  }

  @Test
  def testLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "AND T.len > 1 AND D.age > 20 AND D.name = 'Fabian' " +
      "WHERE T.id > 1"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", null, null),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33),
      BatchTestBase.row(8, 11, "Hello world", null, null),
      BatchTestBase.row(9, 12, "Hello world!", null, null))
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", "Julian"),
      BatchTestBase.row(2, 15, "Hello", "Jark"),
      BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableWithPushDown(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND D.age > 20"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark"),
      BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id WHERE T.len <= D.age"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33))
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableOnMultiFields(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND T.content = D.name"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian"),
      BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableOnMultiFieldsWithUdf(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON mod(T.id, 4) = D.id AND T.content = D.name"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian"),
      BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian"),
      BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", 11),
      BatchTestBase.row(2, 15, "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", 33),
      BatchTestBase.row(8, 11, null, null),
      BatchTestBase.row(9, 12, null, null))
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM nullableT T JOIN userTableWithNull " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(3,15,"Fabian"))
    checkResult(sql, expected)
  }

  @Test
  def testLeftJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = s"SELECT D.id, T.len, D.name FROM nullableT T LEFT JOIN userTableWithNull " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"
    val expected = Seq(
      BatchTestBase.row(null,15,null),
      BatchTestBase.row(3,15,"Fabian"),
      BatchTestBase.row(null,11,null),
      BatchTestBase.row(null,12,null))
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableOnNullConstantKey(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON D.id = null"
    val expected = Seq()
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithNullConstantKey(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND null = D.id"
    val expected = Seq()
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableWithComputedColumn(): Unit = {
    //Computed column do not support in legacyTableSource.
    Assume.assumeFalse(legacyTableSource)

    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age, D.nominal_age " +
      "FROM T JOIN userTableWithComputedColumn " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", "Julian", 11, 12),
      BatchTestBase.row(2, 15, "Hello", "Jark", 22, 23),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33, 34))
    checkResult(sql, expected)
  }

  @Test
  def testJoinTemporalTableWithComputedColumnAndPushDown(): Unit = {
    //Computed column do not support in legacyTableSource.
    Assume.assumeFalse(legacyTableSource)

    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age, D.nominal_age " +
      "FROM T JOIN userTableWithComputedColumn " +
      "for system_time as of T.proctime AS D ON T.id = D.id and D.nominal_age > 12"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark", 22, 23),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33, 34))
    checkResult(sql, expected)
  }
}

object LookupJoinITCase {

  @Parameterized.Parameters(name = "LegacyTableSource={0}, isAsyncMode = {1}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(JBoolean.TRUE, JBoolean.TRUE),
      Array(JBoolean.TRUE, JBoolean.FALSE),
      Array(JBoolean.FALSE, JBoolean.TRUE),
      Array(JBoolean.FALSE, JBoolean.FALSE)
    )
  }
}

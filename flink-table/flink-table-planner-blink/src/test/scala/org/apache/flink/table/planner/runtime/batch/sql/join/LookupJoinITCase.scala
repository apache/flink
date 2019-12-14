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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.planner.runtime.utils.{BatchTableEnvUtil, BatchTestBase, InMemoryLookupableTableSource}

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.lang.Boolean
import java.util

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class LookupJoinITCase(isAsyncMode: Boolean) extends BatchTestBase {

  val data = List(
    BatchTestBase.row(1L, 12L, "Julian"),
    BatchTestBase.row(2L, 15L, "Hello"),
    BatchTestBase.row(3L, 15L, "Fabian"),
    BatchTestBase.row(8L, 11L, "Hello world"),
    BatchTestBase.row(9L, 12L, "Hello world!"))

  val dataWithNull = List(
    BatchTestBase.row(null, 15L, "Hello"),
    BatchTestBase.row(3L, 15L, "Fabian"),
    BatchTestBase.row(null, 11L, "Hello world"),
    BatchTestBase.row(9L, 12L, "Hello world!"))

  val typeInfo = new RowTypeInfo(LONG_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)

  val userData = List(
    (11, 1L, "Julian"),
    (22, 2L, "Jark"),
    (33, 3L, "Fabian"))

  val userTableSource = InMemoryLookupableTableSource.builder()
    .data(userData)
    .field("age", Types.INT)
    .field("id", Types.LONG)
    .field("name", Types.STRING)
    .build()

  val userAsyncTableSource = InMemoryLookupableTableSource.builder()
    .data(userData)
    .field("age", Types.INT)
    .field("id", Types.LONG)
    .field("name", Types.STRING)
    .enableAsync()
    .build()

  val userDataWithNull = List(
    (11, 1L, "Julian"),
    (22, null, "Hello"),
    (33, 3L, "Fabian"),
    (44, null, "Hello world"))

  val userWithNullDataTableSource = InMemoryLookupableTableSource.builder()
    .data(userDataWithNull)
    .field("age", Types.INT)
    .field("id", Types.LONG)
    .field("name", Types.STRING)
    .build()

  val userAsyncWithNullDataTableSource = InMemoryLookupableTableSource.builder()
    .data(userDataWithNull)
    .field("age", Types.INT)
    .field("id", Types.LONG)
    .field("name", Types.STRING)
    .enableAsync()
    .build()

  var userTable: String = _
  var userTableWithNull: String = _

  @Before
  override def before() {
    super.before()
    BatchTableEnvUtil.registerCollection(tEnv, "T0", data, typeInfo, "id, len, content")
    val myTable = tEnv.sqlQuery("SELECT *, PROCTIME() as proctime  FROM T0")
    tEnv.registerTable("T", myTable)

    BatchTableEnvUtil.registerCollection(
      tEnv, "T1", dataWithNull, typeInfo, "id, len, content")
    val myTable1 = tEnv.sqlQuery("SELECT *, PROCTIME() as proctime  FROM T1")
    tEnv.registerTable("nullableT", myTable1)

    tEnv.registerTableSource("userTable", userTableSource)
    tEnv.registerTableSource("userAsyncTable", userAsyncTableSource)
    userTable = if (isAsyncMode) "userAsyncTable" else "userTable"

    tEnv.registerTableSource("userWithNullDataTable", userWithNullDataTableSource)
    tEnv.registerTableSource("userWithNullDataAsyncTable", userAsyncWithNullDataTableSource)
    userTableWithNull = if (isAsyncMode) "userWithNullDataAsyncTable" else "userWithNullDataTable"

    // TODO: enable object reuse until [FLINK-12351] is fixed.
    env.getConfig.disableObjectReuse()
  }

  @Test
  def testLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age FROM T LEFT JOIN $userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id " +
      "AND T.len > 1 AND D.age > 20 AND D.name = 'Fabian' " +
      "WHERE T.id > 1"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", null, null),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33),
      BatchTestBase.row(8, 11, "Hello world", null, null),
      BatchTestBase.row(9, 12, "Hello world!", null, null))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name FROM T JOIN $userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", "Julian"),
      BatchTestBase.row(2, 15, "Hello", "Jark"),
      BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableWithPushDown(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name FROM T JOIN $userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND D.age > 20"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark"),
      BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN $userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id WHERE T.len <= D.age"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableOnMultiFields(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN $userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id AND T.content = D.name"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian"),
      BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableOnMultiFieldsWithUdf(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN $userTable " +
      "for system_time as of T.proctime AS D ON mod(T.id, 4) = D.id AND T.content = D.name"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian"),
      BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN $userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian"),
      BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN $userTable " +
      "for system_time as of T.proctime AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", 11),
      BatchTestBase.row(2, 15, "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", 33),
      BatchTestBase.row(8, 11, null, null),
      BatchTestBase.row(9, 12, null, null))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM nullableT T JOIN $userTableWithNull " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(3,15,"Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testLeftJoinTemporalTableOnMultiKeyFieldsWithNullData(): Unit = {
    val sql = s"SELECT D.id, T.len, D.name FROM nullableT T LEFT JOIN $userTableWithNull " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND T.id = D.id"
    val expected = Seq(
      BatchTestBase.row(null,15,null),
      BatchTestBase.row(3,15,"Fabian"),
      BatchTestBase.row(null,11,null),
      BatchTestBase.row(null,12,null))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableOnNullConstantKey(): Unit = {
    val sql = s"SELECT T.id, T.len, T.content FROM T JOIN $userTable " +
      "for system_time as of T.proctime AS D ON D.id = null"
    val expected = Seq()
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithNullConstantKey(): Unit = {
    val sql = s"SELECT T.id, T.len, D.name FROM T JOIN $userTable " +
      "for system_time as of T.proctime AS D ON T.content = D.name AND null = D.id"
    val expected = Seq()
    checkResult(sql, expected, false)
  }
}

object LookupJoinITCase {

  @Parameterized.Parameters(name = "isAsyncMode = {0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(Boolean.TRUE), Array(Boolean.FALSE)
    )
  }
}

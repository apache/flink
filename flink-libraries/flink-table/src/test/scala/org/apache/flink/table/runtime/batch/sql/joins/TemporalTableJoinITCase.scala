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
package org.apache.flink.table.runtime.batch.sql.joins

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.utils.TemporalTableUtils._
import org.junit.{Before, Test}

class TemporalTableJoinITCase extends BatchTestBase {

  val data = List(
    BatchTestBase.row(1, 12L, "Julian"),
    BatchTestBase.row(2, 15L, "Hello"),
    BatchTestBase.row(3, 15L, "Fabian"),
    BatchTestBase.row(8, 11L, "Hello world"),
    BatchTestBase.row(9, 12L, "Hello world!"))

  val typeInfo = new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)

  @Before
  def setup() {
    tEnv.registerCollection("T", data, typeInfo, 'id, 'len, 'content)
    val temporalTable = new TestingTemporalTableSource
    tEnv.registerTableSource("csvTemporal", temporalTable)
  }


  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", "Julian"),
      BatchTestBase.row(2, 15, "Hello", "Jark"),
      BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableWithPushDown(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id AND D.age > 20"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark"),
      BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id WHERE T.len <= D.age"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableOnMultiFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id AND T.content = D.name"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian"),
      BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.content = D.name AND T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian"),
      BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", 11),
      BatchTestBase.row(2, 15, "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", 33),
      BatchTestBase.row(8, 11, null, null),
      BatchTestBase.row(9, 12, null, null))
    checkResult(sql, expected, false)
  }

  @Test
  def testAsyncJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", "Julian"),
      BatchTestBase.row(2, 15, "Hello", "Jark"),
      BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testAsyncJoinTemporalTableWithPushDown(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id AND D.age > 20"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark"),
      BatchTestBase.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testAsyncJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id WHERE T.len <= D.age"

    val expected = Seq(
      BatchTestBase.row(2, 15, "Hello", "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", "Fabian", 33))
    checkResult(sql, expected, false)
  }

  @Test
  def testAsyncLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T LEFT JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id " +
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
  def testAsyncJoinTemporalTableOnMultiFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id AND T.content = D.name"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian"),
      BatchTestBase.row(3, 15, "Fabian"))
    checkResult(sql, expected, false)
  }

  @Test
  def testAsyncLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvTemporal " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val expected = Seq(
      BatchTestBase.row(1, 12, "Julian", 11),
      BatchTestBase.row(2, 15, "Jark", 22),
      BatchTestBase.row(3, 15, "Fabian", 33),
      BatchTestBase.row(8, 11, null, null),
      BatchTestBase.row(9, 12, null, null))
    checkResult(sql, expected, false)
  }
}

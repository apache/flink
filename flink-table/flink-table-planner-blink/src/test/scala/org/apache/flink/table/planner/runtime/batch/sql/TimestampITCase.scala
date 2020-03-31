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

import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.table.planner.utils.TestDataTypeTableSource
import org.apache.flink.types.Row
import org.junit.Test

import java.sql.Timestamp
import java.time.LocalDateTime

import scala.collection.mutable

class TimestampITCase extends BatchTestBase {

  override def before(): Unit = {
    super.before()

    val tableSchema = TableSchema.builder().fields(
      Array("a", "b", "c", "d"),
      Array(
        DataTypes.INT(),
        DataTypes.BIGINT(),
        DataTypes.TIMESTAMP(9).bridgedTo(classOf[LocalDateTime]),
        DataTypes.TIMESTAMP(9).bridgedTo(classOf[Timestamp])
      )
    ).build()

    val ints = List(1, 2, 3, 4, null)

    val longs = List(1L, 2L, 2L, 4L, null)
    
    val datetimes = List(
      localDateTime("1969-01-01 00:00:00.123456789"),
      localDateTime("1970-01-01 00:00:00.123456"),
      localDateTime("1970-01-01 00:00:00.123456"),
      localDateTime("1970-01-01 00:00:00.123"),
      null)

    val timestamps = List(
      Timestamp.valueOf("1969-01-01 00:00:00.123456789"),
      Timestamp.valueOf("1970-01-01 00:00:00.123456"),
      Timestamp.valueOf("1970-01-01 00:00:00.123"),
      Timestamp.valueOf("1972-01-01 00:00:00"),
      null
    )

    val data = new mutable.MutableList[Row]

    for (i <- ints.indices) {
      data += row(ints(i), longs(i), datetimes(i), timestamps(i))
    }

    val tableSource = new TestDataTypeTableSource(
      tableSchema,
      data.seq)
    tEnv.registerTableSource("T", tableSource)
  }

  @Test
  def testGroupBy(): Unit = {
    checkResult(
      "SELECT MAX(a), MIN(a), c FROM T GROUP BY c",
      Seq(
        row(1, 1, "1969-01-01T00:00:00.123456789"),
        row(3, 2, "1970-01-01T00:00:00.123456"),
        row(4, 4, "1970-01-01T00:00:00.123"),
        row(null, null, null)))
  }

  @Test
  def testMaxMinOn(): Unit = {
    checkResult(
      "SELECT MAX(d), MIN(d), b FROM T GROUP BY b",
      Seq(
        row("1969-01-01T00:00:00.123456789", "1969-01-01T00:00:00.123456789", 1),
        row("1970-01-01T00:00:00.123456", "1970-01-01T00:00:00.123", 2),
        row("1972-01-01T00:00", "1972-01-01T00:00", 4),
        row(null, null, null)
      ))
  }

  @Test
  def testLeadLagOn(): Unit = {
    checkResult(
      "SELECT b, d, lag(d) OVER (PARTITION BY b ORDER BY d), " +
        "LEAD(d) OVER (PARTITION BY b ORDER BY d) FROM T",
      Seq(
        row(1, "1969-01-01T00:00:00.123456789", null, null),
        row(2, "1970-01-01T00:00:00.123", null, "1970-01-01T00:00:00.123456"),
        row(2, "1970-01-01T00:00:00.123456", "1970-01-01T00:00:00.123", null),
        row(4, "1972-01-01T00:00", null, null),
        row(null, null, null, null)
      ))
  }

  @Test
  def testJoinOn(): Unit = {
    checkResult(
      "SELECT * FROM T as T1 JOIN T as T2 ON T1.c = T2.d",
      Seq(
        row(1, 1, "1969-01-01T00:00:00.123456789", "1969-01-01T00:00:00.123456789",
          1, 1, "1969-01-01T00:00:00.123456789", "1969-01-01T00:00:00.123456789"),
        row(2, 2, "1970-01-01T00:00:00.123456", "1970-01-01T00:00:00.123456",
          2, 2, "1970-01-01T00:00:00.123456", "1970-01-01T00:00:00.123456"),
        row(3, 2, "1970-01-01T00:00:00.123456", "1970-01-01T00:00:00.123",
          2, 2, "1970-01-01T00:00:00.123456", "1970-01-01T00:00:00.123456"),
        row(4, 4, "1970-01-01T00:00:00.123", "1972-01-01T00:00",
          3, 2, "1970-01-01T00:00:00.123456", "1970-01-01T00:00:00.123")
      ))
  }

}

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
import java.time.{Instant, LocalDateTime, ZoneId}

import scala.collection.mutable

class TimestampITCase extends BatchTestBase {

  override def before(): Unit = {
    super.before()

    val tableSchema = TableSchema.builder().fields(
      Array("a", "b", "c", "d", "e", "f", "g", "h"),
      Array(
        DataTypes.INT(),
        DataTypes.BIGINT(),
        DataTypes.TIMESTAMP(9).bridgedTo(classOf[LocalDateTime]),
        DataTypes.TIMESTAMP(9).bridgedTo(classOf[Timestamp]),
        DataTypes.TIMESTAMP(3).bridgedTo(classOf[LocalDateTime]),
        DataTypes.TIMESTAMP(3).bridgedTo(classOf[Timestamp]),
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9).bridgedTo(classOf[Instant]),
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9).bridgedTo(classOf[Instant]))
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

    val datetimesWithMilli = List(
      localDateTime("1969-01-01 00:00:00.123"),
      localDateTime("1970-01-01 00:00:00.12"),
      localDateTime("1970-01-01 00:00:00.12"),
      localDateTime("1970-01-01 00:00:00.1"),
      null)

    val timestampsWithMilli = List(
      Timestamp.valueOf("1969-01-01 00:00:00.123"),
      Timestamp.valueOf("1970-01-01 00:00:00.12"),
      Timestamp.valueOf("1970-01-01 00:00:00.1"),
      Timestamp.valueOf("1972-01-01 00:00:00"),
      null
    )

    val instantsOfDateTime = new mutable.MutableList[Instant]
    for (i <- datetimes.indices) {
      if (datetimes(i) == null) {
        instantsOfDateTime += null
      } else {
        // Assume the time zone of source side is UTC
        instantsOfDateTime +=
          datetimes(i).toInstant(ZoneId.of("UTC").getRules.getOffset(datetimes(i)))
      }
    }

    val instantsOfTimestamp = new mutable.MutableList[Instant]
    for (i <- timestamps.indices) {
      if (timestamps(i) == null) {
        instantsOfTimestamp += null
      } else {
        // Assume the time zone of source side is UTC
        val ldt = timestamps(i).toLocalDateTime
        instantsOfTimestamp += ldt.toInstant(ZoneId.of("UTC").getRules.getOffset(ldt))
      }
    }


    val data = new mutable.MutableList[Row]

    for (i <- ints.indices) {
      data += row(ints(i), longs(i), datetimes(i), timestamps(i), datetimesWithMilli(i),
        timestampsWithMilli(i), instantsOfDateTime(i), instantsOfTimestamp(i))
    }

    val tableSource = new TestDataTypeTableSource(
      tableSchema,
      data.seq)
    tEnv.registerTableSource("T", tableSource)
  }

  @Test
  def testGroupBy(): Unit = {
    // group by TIMESTAMP(9)
    checkResult(
      "SELECT MAX(a), MIN(a), c FROM T GROUP BY c",
      Seq(
        row(1, 1, "1969-01-01T00:00:00.123456789"),
        row(3, 2, "1970-01-01T00:00:00.123456"),
        row(4, 4, "1970-01-01T00:00:00.123"),
        row(null, null, null)))

    // group by TIMESTAMP(3)
    checkResult(
      "SELECT MAX(a), MIN(a), e FROM T GROUP BY e",
      Seq(
        row(1, 1, "1969-01-01T00:00:00.123"),
        row(3, 2, "1970-01-01T00:00:00.120"),
        row(4, 4, "1970-01-01T00:00:00.100"),
        row(null, null, null)))

    // group by TIMESTAMP(9) WITH LOCAL TIME ZONE
    checkResult(
      "SELECT MAX(a), MIN(a), g FROM T GROUP BY g",
      Seq(
        row(1, 1, "1969-01-01T00:00:00.123456789Z"),
        row(3, 2, "1970-01-01T00:00:00.123456Z"),
        row(4, 4, "1970-01-01T00:00:00.123Z"),
        row(null, null, null))
    )

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
    // Join On TIMESTAMP(9)
    checkResult(
      "SELECT T1.a, T1.b, T1.c, T1.d, T2.a, T2.b, T2.c, T2.d FROM T as T1 " +
        "JOIN T as T2 ON T1.c = T2.d",
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

    // Join on TIMESTAMP(3)
    checkResult(
      "SELECT T1.a, T1.b, T1.e, T1.f, T2.a, T2.b, T2.e, T2.f FROM T as T1 " +
        "JOIN T as T2 ON T1.e = T2.f",
      Seq(
        row(1, 1, "1969-01-01T00:00:00.123", "1969-01-01T00:00:00.123",
          1, 1, "1969-01-01T00:00:00.123", "1969-01-01T00:00:00.123"),
        row(2, 2, "1970-01-01T00:00:00.120", "1970-01-01T00:00:00.120",
          2, 2, "1970-01-01T00:00:00.120", "1970-01-01T00:00:00.120"),
        row(3, 2, "1970-01-01T00:00:00.120", "1970-01-01T00:00:00.100",
          2, 2, "1970-01-01T00:00:00.120", "1970-01-01T00:00:00.120"),
        row(4, 4, "1970-01-01T00:00:00.100", "1972-01-01T00:00",
          3, 2, "1970-01-01T00:00:00.120", "1970-01-01T00:00:00.100")
      ))

    // Join on TIMESTAMP(9) WITH LOCAL TIME ZONE
    checkResult(
      "SELECT T1.a, T1.b, T1.g, T1.h, T2.a, T2.b, T2.g, T2.h " +
        "FROM T as T1 JOIN T as T2 ON T1.g = T2.h",
      Seq(
        row(1, 1, "1969-01-01T00:00:00.123456789Z", "1969-01-01T00:00:00.123456789Z",
          1, 1, "1969-01-01T00:00:00.123456789Z", "1969-01-01T00:00:00.123456789Z"),
        row(2, 2, "1970-01-01T00:00:00.123456Z", "1970-01-01T00:00:00.123456Z",
          2, 2, "1970-01-01T00:00:00.123456Z", "1970-01-01T00:00:00.123456Z"),
        row(3, 2, "1970-01-01T00:00:00.123456Z", "1970-01-01T00:00:00.123Z",
          2, 2, "1970-01-01T00:00:00.123456Z", "1970-01-01T00:00:00.123456Z"),
        row(4, 4, "1970-01-01T00:00:00.123Z", "1972-01-01T00:00:00Z",
          3, 2, "1970-01-01T00:00:00.123456Z", "1970-01-01T00:00:00.123Z")
      ))
  }

}

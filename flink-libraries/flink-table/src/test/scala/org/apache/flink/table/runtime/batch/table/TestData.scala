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

package org.apache.flink.table.runtime.batch.table

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{
  DOUBLE_TYPE_INFO, INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.{DATE, TIME, TIMESTAMP}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.util.DateTimeTestUtil.{UTCDate, UTCTime, UTCTimestamp}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.types.Row

import scala.collection.Seq

object TestData {
  lazy val data1 = Seq(
    row(2, "a", 6),
    row(4, "b", 8),
    row(6, "c", 10),
    row(1, "a", 5),
    row(3, "b", 7),
    row(5, "c", 9)
  )

  lazy val data2 = Seq(
    row(2, 3L, 2, "Hallo Welt wie", 1L),
    row(3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    row(2, 2L, 1, "Hallo Welt", 2L),
    row(1, 1L, 0, "Hallo", 1L),
    row(5, 11L, 10, "GHI", 1L),
    row(3, 5L, 4, "ABC", 2L),
    row(4, 10L, 9, "FGH", 2L),
    row(4, 7L, 6, "CDE", 2L),
    row(5, 14L, 13, "JKL", 2L),
    row(4, 9L, 8, "EFG", 1L),
    row(5, 15L, 14, "KLM", 2L),
    row(5, 12L, 11, "HIJ", 3L),
    row(4, 8L, 7, "DEF", 1L),
    row(5, 13L, 12, "IJK", 3L),
    row(3, 6L, 5, "BCD", 3L)
  )

  lazy val data3 = Seq(
    row(1, 1.1, "a", UTCDate("2017-04-08"), UTCTime("12:00:59"),
      UTCTimestamp("2015-05-20 10:00:00")),
    row(2, 2.5, "abc", UTCDate("2017-04-09"), UTCTime("12:00:59"),
      UTCTimestamp("2019-09-19 08:03:09")),
    row(2, -2.4, "abcd", UTCDate("2017-04-08"), UTCTime("00:00:00"),
      UTCTimestamp("2016-09-01 23:07:06")),
    row(3, 0.0, "abc?", UTCDate("2017-10-11"), UTCTime("23:59:59"),
      UTCTimestamp("1999-12-12 10:00:00")),
    row(3, -9.77, "ABC", UTCDate("2016-08-08"), UTCTime("04:15:00"),
      UTCTimestamp("1999-12-12 10:00:02")),
    row(3, 0.08, "BCD", UTCDate("2017-04-10"), UTCTime("02:30:00"),
      UTCTimestamp("1999-12-12 10:03:00")),
    row(4, 3.14, "CDE", UTCDate("2017-11-11"), UTCTime("02:30:00"),
      UTCTimestamp("2017-11-20 09:00:00")),
    row(4, 3.15, "DEF", UTCDate("2017-02-06"), UTCTime("06:00:00"),
      UTCTimestamp("2015-11-19 10:00:00")),
    row(4, 3.14, "EFG", UTCDate("2017-05-20"), UTCTime("09:45:78"),
      UTCTimestamp("2015-11-19 10:00:01")),
    row(4, 3.16, "FGH", UTCDate("2017-05-19"), UTCTime("11:11:11"),
      UTCTimestamp("2015-11-20 08:59:59")),
    row(5, -5.9, "GHI", UTCDate("2017-07-20"), UTCTime("22:22:22"),
      UTCTimestamp("1989-06-04 10:00:00.78")),
    row(5, 2.71, "HIJ", UTCDate("2017-09-08"), UTCTime("20:09:09"),
      UTCTimestamp("1997-07-01 09:00:00.99")),
    row(5, 3.9, "IJK", UTCDate("2017-02-02"), UTCTime("03:03:03"),
      UTCTimestamp("2000-01-01 00:00:00.09")),
    row(5, 0.7, "JKL", UTCDate("2017-10-01"), UTCTime("19:00:00"),
      UTCTimestamp("2010-06-01 10:00:00.999")),
    row(5, -2.8, "KLM", UTCDate("2017-07-01"), UTCTime("12:00:59"),
      UTCTimestamp("1937-07-07 08:08:08.888"))
  )

  lazy val data4 = Seq(
    row("book", 1, 12),
    row("book", 2, 19),
    row("book", 4, 11),
    row("fruit", 4, 33),
    row("fruit", 3, 44),
    row("fruit", 5, 22)
  )

  lazy val data5 = Seq(
    row(1, 0.1),
    row(2, 0.2),
    row(2, 0.2),
    row(3, 0.3),
    row(3, 0.3),
    row(3, 0.4),
    row(4, 0.5),
    row(4, 0.5),
    row(4, 0.6),
    row(4, 0.6),
    row(5, 0.7),
    row(5, 0.7),
    row(5, 0.8),
    row(5, 0.8),
    row(5, 0.9)
  )

  lazy val nullData2: Seq[Row] = data2 ++ Seq(
    row(null, 3L, 3, "NullTuple", 3L),
    row(null, 3L, 3, "NullTuple", 3L)
  )

  lazy val nullData4: Seq[Row] = Seq(
    row("book", 1, 12),
    row("book", 2, null),
    row("book", 4, 11),
    row("fruit", 4, null),
    row("fruit", 3, 44),
    row("fruit", 5, null)
  )

  val type1 = new RowTypeInfo(INT_TYPE_INFO, STRING_TYPE_INFO, INT_TYPE_INFO)

  val type2 = new RowTypeInfo(
    INT_TYPE_INFO, LONG_TYPE_INFO, INT_TYPE_INFO, STRING_TYPE_INFO, LONG_TYPE_INFO)

  val type3 = new RowTypeInfo(
    INT_TYPE_INFO, DOUBLE_TYPE_INFO, STRING_TYPE_INFO, DATE, TIME, TIMESTAMP)

  val type4 = new RowTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO)

  val type5 = new RowTypeInfo(INT_TYPE_INFO, DOUBLE_TYPE_INFO)

  val nullablesOfData1 = Seq(false, false, false)

  val nullablesOfData2 = Seq(false, false, false, false, false)

  val nullablesOfNullData2 = Seq(true, false, false, false, false)

  val nullablesOfData3 = Seq(false, false, false, false, false, false)

  val nullablesOfData4 = Seq(false, false, false)

  val nullablesOfNullData4 = Seq(false, false, true)

  val nullablesOfData5 = Seq(false, false)
}

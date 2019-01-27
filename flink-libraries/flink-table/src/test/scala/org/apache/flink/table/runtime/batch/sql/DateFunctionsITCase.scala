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

package org.apache.flink.table.runtime.batch.sql

import java.sql.Date

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData.{buildInData, buildInType}
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions
import org.apache.flink.table.util.DateTimeTestUtil._
import org.junit.{Assert, Before, Ignore, Test}

class DateFunctionsITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("testTable", buildInData, buildInType, "a,b,c,d,e,f,g,h,i,j")
  }

  @Test
  def testCurrentDate(): Unit = {
    // Execution in on Query should return the same value
    checkResult("SELECT CURRENT_DATE = CURRENT_DATE FROM testTable WHERE a = TRUE",
      Seq(row(true)))

    val d0 = BuildInScalarFunctions.toInt(new Date(System.currentTimeMillis()))

    val table = parseQuery("SELECT CURRENT_DATE FROM testTable WHERE a = TRUE")
    val result = executeQuery(table)
    val d1 = BuildInScalarFunctions.toInt(result.toList(0).getField(0).asInstanceOf[java.sql.Date])

    Assert.assertTrue(d0 <= d1 && d1 - d0 <= 1)
  }

  @Test
  def testCurrentTimestamp(): Unit = {
    // Execution in on Query should return the same value
    checkResult("SELECT CURRENT_TIMESTAMP = CURRENT_TIMESTAMP FROM testTable WHERE a = TRUE",
      Seq(row(true)))

    // CURRENT_TIMESTAMP should return the current timestamp
    val ts0 = System.currentTimeMillis()

    val table = parseQuery("SELECT CURRENT_TIMESTAMP FROM testTable WHERE a = TRUE")
    val result = executeQuery(table)
    val ts1 = BuildInScalarFunctions.toLong(
                result.toList(0).getField(0).asInstanceOf[java.sql.Timestamp])

    val ts2 = System.currentTimeMillis()

    Assert.assertTrue(ts0 <= ts1 && ts1 <= ts2)
  }

  @Test
  def testCurrentTime(): Unit = {
    // Execution in on Query should return the same value
    checkResult("SELECT CURRENT_TIME = CURRENT_TIME FROM testTable WHERE a = TRUE",
      Seq(row(true)))
  }

  def testTimestampCompareWithDate(): Unit = {
    checkResult("SELECT j FROM testTable WHERE j < DATE '2017-11-11'",
      Seq(row(true)))
  }

  @Test
  def testTimestampCompareWithDateString(): Unit = {
    //j 2015-05-20 10:00:00.887
    checkResult("SELECT j FROM testTable WHERE j < '2017-11-11'",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"))))
  }

  @Test
  def testDateCompareWithDateString(): Unit = {
    checkResult("SELECT h FROM testTable WHERE h <= '2017-12-12'",
      Seq(
        row(UTCDate("2017-12-12")),
        row(UTCDate("2017-12-12"))
      ))
  }

  @Ignore
  @Test
  def testDateEqualsWithDateString(): Unit = {
    // TODO: May be a bug
    checkResult("SELECT h FROM testTable WHERE h = '2017-12-12'",
      Seq(
        row(UTCDate("2017-12-12")),
        row(UTCDate("2017-12-12"))
      ))
  }

  @Test
  def testDateFormat(): Unit = {
    //j 2015-05-20 10:00:00.887
    checkResult("SELECT j, " +
      " DATE_FORMAT(j, 'yyyy/MM/dd HH:mm:ss')," +
      " DATE_FORMAT('2015-05-20 10:00:00.887', 'yyyy/MM/dd HH:mm:ss')," +
      " DATE_FORMAT('2015-05-20 10:00:00.887', 'yyyy-MM-dd HH:mm:ss', 'yyyy/MM/dd HH:mm:ss')" +
      " FROM testTable WHERE a = TRUE",
      Seq(
        row(UTCTimestamp("2015-05-20 10:00:00.887"),
          "2015/05/20 10:00:00",
          "2015/05/20 10:00:00",
          "2015/05/20 10:00:00")
      ))
  }

  @Test
  def testYear(): Unit = {
    checkResult("SELECT j, YEAR(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "2015")))
  }

  @Test
  def testQuarter(): Unit = {
    checkResult("SELECT j, QUARTER(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "2")))
  }

  @Test
  def testMonth(): Unit = {
    checkResult("SELECT j, MONTH(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "5")))
  }

  @Test
  def testWeek(): Unit = {
    checkResult("SELECT j, WEEK(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "21")))
  }

  @Test
  def testDayOfYear(): Unit = {
    checkResult("SELECT j, DAYOFYEAR(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "140")))
  }

  @Test
  def testDayOfMonth(): Unit = {
    checkResult("SELECT j, DAYOFMONTH(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "20")))
  }

  @Test
  def testDayOfWeek(): Unit = {
    checkResult("SELECT j, DAYOFWEEK(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "4")))
  }

  @Test
  def testHour(): Unit = {
    checkResult("SELECT j, HOUR(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "10")))
  }

  @Test
  def testMinute(): Unit = {
    checkResult("SELECT j, MINUTE(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "0")))
  }

  @Test
  def testSecond(): Unit = {
    checkResult("SELECT j, SECOND(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "0")))
  }

  @Test
  def testUnixTimestamp(): Unit = {
    checkResult("SELECT" +
      " UNIX_TIMESTAMP('2017-12-13 19:25:30')," +
      " UNIX_TIMESTAMP('2017-12-13 19:25:30', 'yyyy-MM-dd HH:mm:ss')" +
      " FROM testTable WHERE a = TRUE",
      Seq(row(1513193130, 1513193130)))
  }

  @Test
  def testFromUnixTime(): Unit = {
    checkResult("SELECT" +
      " FROM_UNIXTIME(1513193130), FROM_UNIXTIME(1513193130, 'MM/dd/yyyy HH:mm:ss')" +
      " FROM testTable WHERE a = TRUE",
      Seq(row("2017-12-13 19:25:30", "12/13/2017 19:25:30")))
  }

  @Test
  def testDateDiff(): Unit = {
    checkResult("SELECT" +
      " DATEDIFF('2017-12-14 01:00:34', '2016-12-14 12:00:00')," +
      " DATEDIFF(TIMESTAMP '2017-12-14 01:00:23', '2016-08-14 12:00:00')," +
      " DATEDIFF('2017-12-14 09:00:23', TIMESTAMP '2013-08-19 11:00:00')," +
      " DATEDIFF(TIMESTAMP '2017-12-14 09:00:23', TIMESTAMP '2018-08-19 11:00:00')" +
      " FROM testTable WHERE a = TRUE",
      Seq(row(365, 487, 1578, -248)))
  }

  @Test
  def testDateSub(): Unit = {
    checkResult("SELECT" +
      " DATE_SUB(TIMESTAMP '2017-12-14 09:00:23', 3)," +
      " DATE_SUB('2017-12-14 09:00:23', -3)" +
      " FROM testTable WHERE a = TRUE",
      Seq(row("2017-12-11", "2017-12-17")))
  }

  @Test
  def testDateAdd(): Unit = {
    checkResult("SELECT" +
      " DATE_ADD('2017-12-14', 4)," +
      " DATE_ADD(TIMESTAMP '2017-12-14 09:10:20',-4)" +
      " FROM testTable WHERE a = TRUE",
      Seq(row("2017-12-18", "2017-12-10")))
  }

  @Test
  def testToDate(): Unit = {
    checkResult("SELECT" +
      " TO_DATE(CAST(null AS VARCHAR))," +
      " TO_DATE('2016-12-31')," +
      " TO_DATE('2016-12-31', 'yyyy-MM-dd')",
      Seq(row(null, UTCDate("2016-12-31"), UTCDate("2016-12-31"))))
  }

  @Test
  def testToTimestamp(): Unit = {
    checkResult("SELECT" +
      " TO_TIMESTAMP(CAST(null AS VARCHAR))," +
      " TO_TIMESTAMP('2016-12-31 00:12:00')," +
      " TO_TIMESTAMP('2016-12-31', 'yyyy-MM-dd')",
      Seq(row(null, UTCTimestamp("2016-12-31 00:12:00"), UTCTimestamp("2016-12-31 00:00:00"))))
  }
}

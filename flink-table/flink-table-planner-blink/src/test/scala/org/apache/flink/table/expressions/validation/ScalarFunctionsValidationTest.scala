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

package org.apache.flink.table.expressions.validation

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.flink.table.api.{SqlParserException, ValidationException}
import org.apache.flink.table.expressions.utils.ScalarTypesTestBase
import org.junit.{Ignore, Test}

class ScalarFunctionsValidationTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------

  @Ignore("TODO: FLINK-11898")
  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidLog1(): Unit = {
    // invalid arithmetic argument
    testSqlApi(
      "LOG(1, 100)",
      "FAIL"
    )
  }

  @Ignore("TODO: FLINK-11898")
  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidLog2(): Unit ={
    // invalid arithmetic argument
    testSqlApi(
      "LOG(-1)",
      "FAIL"
    )
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidBin1(): Unit = {
    testSqlApi("BIN(f12)", "101010") // float type
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidBin2(): Unit = {
    testSqlApi("BIN(f15)", "101010") // BigDecimal type
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidBin3(): Unit = {
    testSqlApi("BIN(f16)", "101010") // Date type
  }


  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  @Test(expected = classOf[SqlParserException])
  def testTimestampAddWithWrongTimestampInterval(): Unit ={
    testSqlApi("TIMESTAMPADD(XXX, 1, timestamp '2016-02-24'))", "2016-06-16")
  }

  @Test(expected = classOf[SqlParserException])
  def testTimestampAddWithWrongTimestampFormat(): Unit ={
    testSqlApi("TIMESTAMPADD(YEAR, 1, timestamp '2016-02-24'))", "2016-06-16")
  }

  @Test(expected = classOf[ValidationException])
  def testTimestampAddWithWrongQuantity(): Unit ={
    testSqlApi("TIMESTAMPADD(YEAR, 1.0, timestamp '2016-02-24 12:42:25')", "2016-06-16")
  }

  // ----------------------------------------------------------------------------------------------
  // Sub-query functions
  // ----------------------------------------------------------------------------------------------

  @Ignore("TODO: FLINK-11898")
  @Test(expected = classOf[ValidationException])
  def testDOWWithTimeWhichIsUnsupported(): Unit = {
    testSqlApi("EXTRACT(DOW FROM TIME '12:42:25')", "0")
  }

  @Ignore("TODO: FLINK-11898")
  @Test(expected = classOf[ValidationException])
  def testDOYWithTimeWhichIsUnsupported(): Unit = {
    testSqlApi("EXTRACT(DOY FROM TIME '12:42:25')", "0")
  }

  private def testExtractFromTimeZeroResult(unit: TimeUnit): Unit = {
    testSqlApi("EXTRACT(" + unit + " FROM TIME '00:00:00')", "0")
  }

  @Ignore("TODO: FLINK-11898")
  @Test(expected = classOf[ValidationException])
  def testMillenniumWithTime(): Unit = {
    testExtractFromTimeZeroResult(TimeUnit.MILLENNIUM)
  }

  @Ignore("TODO: FLINK-11898")
  @Test(expected = classOf[ValidationException])
  def testCenturyWithTime(): Unit = {
    testExtractFromTimeZeroResult(TimeUnit.CENTURY)
  }

  @Ignore("TODO: FLINK-11898")
  @Test(expected = classOf[ValidationException])
  def testYearWithTime(): Unit = {
    testExtractFromTimeZeroResult(TimeUnit.YEAR)
  }

  @Ignore("TODO: FLINK-11898")
  @Test(expected = classOf[ValidationException])
  def testMonthWithTime(): Unit = {
    testExtractFromTimeZeroResult(TimeUnit.MONTH)
  }

  @Ignore("TODO: FLINK-11898")
  @Test(expected = classOf[ValidationException])
  def testDayWithTime(): Unit = {
    testExtractFromTimeZeroResult(TimeUnit.DAY)
  }
}

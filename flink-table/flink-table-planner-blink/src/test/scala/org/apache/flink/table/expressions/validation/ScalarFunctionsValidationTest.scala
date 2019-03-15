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
import org.junit.Test

class ScalarFunctionsValidationTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testInvalidBin1(): Unit = {
    thrown.expect(classOf[ValidationException])
    testSqlApi("BIN(f12)", "101010") // float type
  }

  @Test
  def testInvalidBin2(): Unit = {
    thrown.expect(classOf[ValidationException])
    testSqlApi("BIN(f15)", "101010") // BigDecimal type
  }

  @Test
  def testInvalidBin3(): Unit = {
    thrown.expect(classOf[ValidationException])
    testSqlApi("BIN(f16)", "101010") // Date type
  }


  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testTimestampAddWithWrongTimestampInterval(): Unit = {
    thrown.expect(classOf[SqlParserException])
    testSqlApi("TIMESTAMPADD(XXX, 1, timestamp '2016-02-24'))", "2016-06-16")
  }

  @Test
  def testTimestampAddWithWrongTimestampFormat(): Unit = {
    thrown.expect(classOf[SqlParserException])
    thrown.expectMessage("Illegal TIMESTAMP literal '2016-02-24'")
    testSqlApi("TIMESTAMPADD(YEAR, 1, timestamp '2016-02-24'))", "2016-06-16")
  }

  @Test
  def testTimestampAddWithWrongQuantity(): Unit = {
    thrown.expect(classOf[ValidationException])
    testSqlApi("TIMESTAMPADD(YEAR, 1.0, timestamp '2016-02-24 12:42:25')", "2016-06-16")
  }

  // ----------------------------------------------------------------------------------------------
  // Sub-query functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testDOWWithTimeWhichIsUnsupported(): Unit = {
    thrown.expect(classOf[ValidationException])
    testSqlApi("EXTRACT(DOW FROM TIME '12:42:25')", "0")
  }

  @Test
  def testDOYWithTimeWhichIsUnsupported(): Unit = {
    thrown.expect(classOf[ValidationException])
    testSqlApi("EXTRACT(DOY FROM TIME '12:42:25')", "0")
  }

  private def testExtractFromTimeZeroResult(unit: TimeUnit): Unit = {
    thrown.expect(classOf[ValidationException])
    testSqlApi("EXTRACT(" + unit + " FROM TIME '00:00:00')", "0")
  }

  @Test
  def testMillenniumWithTime(): Unit = {
    thrown.expect(classOf[ValidationException])
    testExtractFromTimeZeroResult(TimeUnit.MILLENNIUM)
  }

  @Test
  def testCenturyWithTime(): Unit = {
    thrown.expect(classOf[ValidationException])
    testExtractFromTimeZeroResult(TimeUnit.CENTURY)
  }

  @Test
  def testYearWithTime(): Unit = {
    thrown.expect(classOf[ValidationException])
    testExtractFromTimeZeroResult(TimeUnit.YEAR)
  }

  @Test
  def testMonthWithTime(): Unit = {
    thrown.expect(classOf[ValidationException])
    testExtractFromTimeZeroResult(TimeUnit.MONTH)
  }

  @Test
  def testDayWithTime(): Unit = {
    thrown.expect(classOf[ValidationException])
    testExtractFromTimeZeroResult(TimeUnit.DAY)
  }

  // ----------------------------------------------------------------------------------------------
  // Builtin functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testInvalidStringToMap(): Unit = {
    // test non-exist key access
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Invalid number of arguments to function 'STR_TO_MAP'")
    testSqlApi(
      "STR_TO_MAP('k1:v1;k2:v2', ';')",
      "EXCEPTION"
    )
  }

  @Test
  def testInvalidIf(): Unit = {
    // test IF(BOOL, STRING, BOOLEAN)
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Cannot apply 'IF' to arguments")
    testSqlApi(
      "IF(f7 > 5, f0, f1)",
      "FAIL")
  }
}

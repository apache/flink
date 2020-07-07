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

package org.apache.flink.table.planner.expressions.validation

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.TimePointUnit
import org.apache.flink.table.planner.codegen.CodeGenException
import org.apache.flink.table.planner.expressions.utils.ScalarTypesTestBase

import org.apache.calcite.avatica.util.TimeUnit
import org.junit.Test

class ScalarFunctionsValidationTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testInvalidLog1(): Unit = {
    testSqlApi(
      "LOG(1, 100)",
      "Infinity"
    )
  }

  @Test
  def testInvalidLog2(): Unit ={
    testSqlApi(
      "LOG(-1)",
      "NaN"
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


  @Test(expected = classOf[ValidationException])
  def testInvalidTruncate1(): Unit = {
    // All arguments are string type
    testSqlApi(
      "TRUNCATE('abc', 'def')",
      "FAIL")

    // The second argument is of type String
    testSqlApi(
      "TRUNCATE(f12, f0)",
      "FAIL")

    // The second argument is of type Float
    testSqlApi(
      "TRUNCATE(f12,f12)",
      "FAIL")

    // The second argument is of type Double
    testSqlApi(
      "TRUNCATE(f12, cast(f28 as DOUBLE))",
      "FAIL")

    // The second argument is of type BigDecimal
    testSqlApi(
      "TRUNCATE(f12,f15)",
      "FAIL")
  }

  @Test
  def testInvalidTruncate2(): Unit = {
    thrown.expect(classOf[CodeGenException])
    // The one argument is of type String
    testSqlApi(
      "TRUNCATE('abc')",
      "FAIL")
  }

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  @Test(expected = classOf[ValidationException])
  def testInvalidSubstring1(): Unit = {
    // Must fail. Parameter of substring must be an Integer not a Double.
    testTableApi("test".substring(2.0.toExpr), "FAIL", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSubstring2(): Unit = {
    // Must fail. Parameter of substring must be an Integer not a String.
    testTableApi("test".substring("test".toExpr), "FAIL", "FAIL")
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

  @Test(expected = classOf[ValidationException])
  def testInValidationExceptionMoreThanOneTypes(): Unit = {
    testTableApi(
      'f2.in('f3, 'f8),
      "f2.in(f3, f8)",
      "true"
    )
    testTableApi(
      'f2.in('f3, 'f4, 4),
      "f2.in(f3, f4, 4)",
      "false"  // OK if all numeric
    )
  }

  @Test(expected = classOf[ValidationException])
  def scalaInValidationExceptionDifferentOperandsTest(): Unit = {
    testTableApi(
      'f1.in("Hi", "Hello world", "Comment#1"),
      "true",
      "true"
    )
  }

  @Test(expected = classOf[ValidationException])
  def javaInValidationExceptionDifferentOperandsTest(): Unit = {
    testTableApi(
      true,
      "f1.in('Hi','Hello world','Comment#1')",
      "true"
    )
  }

  @Test(expected = classOf[ValidationException])
  def testTimestampDiffWithWrongTime(): Unit = {
    testTableApi(
      timestampDiff(TimePointUnit.DAY, "2016-02-24", "2016-02-27"), "FAIL", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testTimestampDiffWithWrongTimeAndUnit(): Unit = {
    testTableApi(
      timestampDiff(TimePointUnit.MINUTE, "2016-02-24", "2016-02-27"), "FAIL", "FAIL")
  }

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

  @Test
  def testInvalidToBase64(): Unit = {
    //test TO_BASE64(INTEGER)
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Cannot apply 'TO_BASE64' to arguments of type 'TO_BASE64(<INTEGER>)'")
    testSqlApi(
      "TO_BASE64(11)",
      "FAIL")
  }
}

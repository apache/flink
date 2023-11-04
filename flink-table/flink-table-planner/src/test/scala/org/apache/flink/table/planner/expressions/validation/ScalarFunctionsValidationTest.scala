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
import org.apache.flink.table.planner.expressions.utils.ScalarTypesTestBase

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class ScalarFunctionsValidationTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testInvalidBin1(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("BIN(f12)", "101010")) // float type
  }

  @Test
  def testInvalidBin2(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("BIN(f15)", "101010")) // BigDecimal type
  }

  @Test
  def testInvalidBin3(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("BIN(f16)", "101010")) // Date type
  }

  @Test
  def testInvalidTruncate1(): Unit = {
    // All arguments are string type
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("TRUNCATE('abc', 'def')", "FAIL"))

    // The second argument is of type String
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("TRUNCATE(f12, f0)", "FAIL"))

    // The second argument is of type Float
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("TRUNCATE(f12,f12)", "FAIL"))

    // The second argument is of type Double
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("TRUNCATE(f12, cast(f28 as DOUBLE))", "FAIL"))

    // The second argument is of type BigDecimal
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("TRUNCATE(f12,f15)", "FAIL"))
  }

  @Test
  def testInvalidTruncate2(): Unit = {
    // The one argument is of type String
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("TRUNCATE('abc')", "FAIL"))
  }

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testInvalidSubstring1(): Unit = {
    // Must fail. Parameter of substring must be an Integer not a Double.
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testTableApi("test".substring(2.0.toExpr), "FAIL"))
  }

  @Test
  def testInvalidSubstring2(): Unit = {
    // Must fail. Parameter of substring must be an Integer not a String.
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testTableApi("test".substring("test".toExpr), "FAIL"))
  }

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testTimestampAddWithWrongTimestampInterval(): Unit = {
    assertThatExceptionOfType(classOf[SqlParserException])
      .isThrownBy(() => testSqlApi("TIMESTAMPADD(XXX, 1, timestamp '2016-02-24'))", "2016-06-16"))
  }

  @Test
  def testTimestampAddWithWrongTimestampFormat(): Unit = {
    assertThatExceptionOfType(classOf[SqlParserException])
      .isThrownBy(() => testSqlApi("TIMESTAMPADD(YEAR, 1, timestamp '2016-02-24'))", "2016-06-16"))
  }

  @Test
  def testTimestampAddWithWrongQuantity(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => testSqlApi("TIMESTAMPADD(YEAR, 1.0, timestamp '2016-02-24 12:42:25')", "2016-06-16"))
  }

  // ----------------------------------------------------------------------------------------------
  // Sub-query functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testInValidationExceptionMoreThanOneTypes(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testTableApi('f2.in('f3, 'f8), "TRUE"))

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testTableApi('f2.in('f3, 'f4, 4), "FALSE"))
  }

  @Test
  def scalaInValidationExceptionDifferentOperandsTest(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testTableApi('f1.in("Hi", "Hello world", "Comment#1"), "TRUE"))
  }

  @Test
  def testTimestampDiffWithWrongTime(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => testTableApi(timestampDiff(TimePointUnit.DAY, "2016-02-24", "2016-02-27"), "FAIL"))
  }

  @Test
  def testTimestampDiffWithWrongTimeAndUnit(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => testTableApi(timestampDiff(TimePointUnit.MINUTE, "2016-02-24", "2016-02-27"), "FAIL"))
  }

  // ----------------------------------------------------------------------------------------------
  // Builtin functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testInvalidStringToMap(): Unit = {
    // test non-exist key access
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          testSqlApi(
            "STR_TO_MAP('k1:v1;k2:v2', ';')",
            "EXCEPTION"
          ))
      .withMessageContaining("Invalid number of arguments to function 'STR_TO_MAP'")
  }

  @Test
  def testInvalidIf(): Unit = {
    // test IF(BOOL, STRING, BOOLEAN)
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("IF(f7 > 5, f0, f1)", "FAIL"))
      .withMessageContaining("Cannot apply 'IF' to arguments")
  }

  @Test
  def testInvalidToBase64(): Unit = {
    // test TO_BASE64(INTEGER)
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("TO_BASE64(11)", "FAIL"))
      .withMessageContaining("Cannot apply 'TO_BASE64' to arguments of type 'TO_BASE64(<INTEGER>)'")
  }
}

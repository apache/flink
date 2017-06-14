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

import org.apache.flink.table.api.{SqlParserException, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.ScalarTypesTestBase
import org.junit.Test

class ScalarFunctionsValidationTest extends ScalarTypesTestBase {

  // ----------------------------------------------------------------------------------------------
  // Math functions
  // ----------------------------------------------------------------------------------------------

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidLog1(): Unit = {
    // invalid arithmetic argument
    testSqlApi(
      "LOG(1, 100)",
      "FAIL"
    )
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidLog2(): Unit ={
    // invalid arithmetic argument
    testSqlApi(
      "LOG(-1)",
      "FAIL"
    )
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

  @Test(expected = classOf[ValidationException])
  def testTimestampAddWithDate(): Unit ={
    testSqlApi("TIMESTAMPADD(DAY, 1, date '2016-06-15')", "2016-06-16")
  }

  @Test(expected = classOf[SqlParserException])
  def testTimestampAddWithrongTimestampInterval(): Unit ={
    testSqlApi("TIMESTAMPADD(XXX, 1, timestamp '2016-02-24'))", "2016-06-16")
  }

  @Test(expected = classOf[SqlParserException])
  def testTimestampAddWithrongTimestampFormat(): Unit ={
    testSqlApi("TIMESTAMPADD(YEAR, 1, timestamp '2016-02-24'))", "2016-06-16")
  }

  @Test(expected = classOf[ValidationException])
  def testTimestampAddWithWrongQuantity(): Unit ={
    testSqlApi("TIMESTAMPADD(YEAR, 1.0, timestamp '2016-02-24 12:42:25')", "2016-06-16")
  }
}

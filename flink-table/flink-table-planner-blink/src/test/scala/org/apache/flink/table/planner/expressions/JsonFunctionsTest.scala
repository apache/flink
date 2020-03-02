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

package org.apache.flink.table.planner.expressions

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.expressions.utils.ScalarTypesTestBase
import org.junit.Assert.assertEquals
import org.junit.Test

class JsonFunctionsTest extends ScalarTypesTestBase {

  @Test
  def testPredicates(): Unit = {
    val malformed = Array(false, false, false, false)
    val jsonObject = Array(true, true, false, false)
    val jsonArray = Array(true, false, true, false)
    val jsonScalar = Array(true, false, false, true)

    // strings
    verifyPredicates("'{}'", jsonObject)
    verifyPredicates("'[]'", jsonArray)
    verifyPredicates("'100'", jsonScalar)
    verifyPredicates("'{]'", malformed)

    // valid fields
    verifyPredicates("f0", malformed)
    verifyPredicates("f59", jsonObject)

    // invalid fields
    verifyException("f1", classOf[ValidationException])
    verifyException("f2", classOf[ValidationException])
    verifyException("f3", classOf[ValidationException])
    verifyException("f4", classOf[ValidationException])
    verifyException("f5", classOf[ValidationException])
    verifyException("f6", classOf[ValidationException])
  }

  /**
   * Utility for verify predicates.
   *
   * @param candidate to be verified, can be a scalar or a column
   * @param expectedValues array of expected values as result of
   *                       (IS_JSON_VALUE, IS_JSON_OBJECT, IS_JSON_ARRAY, IS_JSON_SCALAR)
   */
  private def verifyPredicates(candidate: String, expectedValues: Array[Boolean]): Unit = {
    assert(expectedValues.length == 4)

    testSqlApi(s"$candidate is json value", expectedValues(0).toString)
    testSqlApi(s"$candidate is not json value", (!expectedValues(0)).toString)
    testSqlApi(s"$candidate is json object", expectedValues(1).toString)
    testSqlApi(s"$candidate is not json object", (!expectedValues(1)).toString)
    testSqlApi(s"$candidate is json array", expectedValues(2).toString)
    testSqlApi(s"$candidate is not json array", (!expectedValues(2)).toString)
    testSqlApi(s"$candidate is json scalar", expectedValues(3).toString)
    testSqlApi(s"$candidate is not json scalar", (!expectedValues(3)).toString)
  }

  private def verifyException[T <: Exception](candidate: String, expectedException: Class[T]): Unit = {
    val sqlCandidates = Array(
      s"$candidate is json value",
      s"$candidate is not json value",
      s"$candidate is json object",
      s"$candidate is not json object",
      s"$candidate is json array",
      s"$candidate is not json array",
      s"$candidate is json scalar",
      s"$candidate is not json scalar")

    for (sql <- sqlCandidates) {
      try {
        testSqlApi(sql, "null")
      } catch {
        case e: Exception => assertEquals(e.getClass, expectedException)
      }
    }
  }

}

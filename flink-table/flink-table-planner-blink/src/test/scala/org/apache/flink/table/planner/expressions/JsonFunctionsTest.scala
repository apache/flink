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

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row
import org.hamcrest.Matchers.startsWith
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.rules.ExpectedException

class JsonFunctionsTest extends ExpressionTestBase {

  override def testData: Row = {
    val testData = new Row(10)
    testData.setField(0, "This is a test String.")
    testData.setField(1, true)
    testData.setField(2, 42.toByte)
    testData.setField(3, 43.toShort)
    testData.setField(4, 44.toLong)
    testData.setField(5, 4.5.toFloat)
    testData.setField(6, 4.6)
    testData.setField(7, 3)
    testData.setField(8, """{ "name" : "flink" }""")
    testData.setField(9,
      """{
        | "info":{
        |       "type":1,
        |       "address":{
        |         "town":"Bristol",
        |         "county":"Avon",
        |         "country":"England"
        |       },
        |       "tags":["Sport", "Water polo"]
        |    },
        |    "type":"Basic"
        | }""".stripMargin)
    testData
  }

  override def typeInfo = new RowTypeInfo(
    /* 0 */ Types.STRING,
    /* 1 */ Types.BOOLEAN,
    /* 2 */ Types.BYTE,
    /* 3 */ Types.SHORT,
    /* 4 */ Types.LONG,
    /* 5 */ Types.FLOAT,
    /* 6 */ Types.DOUBLE,
    /* 7 */ Types.INT,
    /* 8 */ Types.STRING,
    /* 9 */ Types.STRING)

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
    verifyPredicates("f8", jsonObject)

    // invalid fields
    verifyException("f1", classOf[ValidationException])
    verifyException("f2", classOf[ValidationException])
    verifyException("f3", classOf[ValidationException])
    verifyException("f4", classOf[ValidationException])
    verifyException("f5", classOf[ValidationException])
    verifyException("f6", classOf[ValidationException])
    verifyException("f7", classOf[ValidationException])
  }

  /**
    * Utility for verify predicates.
    *
    * @param candidate      to be verified, can be a scalar or a column
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

  private def verifyException[T <: Exception](
       candidate: String,
       expectedException: Class[T]
     ): Unit = {
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
      try testSqlApi(sql, "null") catch {
        case e: Exception => assertEquals(e.getClass, expectedException)
      }
    }
  }

  @Test
  def testJsonExists(): Unit = {
    // lax json test
    testSqlApi("json_exists(f9, 'lax $')", "true")
    testSqlApi("json_exists(f9, 'lax $.info.type')", "true")
    testSqlApi("json_exists(f9, 'lax $.info.address.town')", "true")
    testSqlApi("json_exists(f9, 'lax $.info.\"address\"')", "false")
    testSqlApi("json_exists(f9, 'lax $.info.tags')", "true")
    testSqlApi("json_exists(f9, 'lax $.info.type[0]')", "false")
    testSqlApi("json_exists(f9, 'lax $.info.none')", "false")

    // strict + no error
    testSqlApi("json_exists(f9, 'strict $.info.type')", "true")
    testSqlApi("json_exists(f9, 'strict $.info.type1')", "false")
    // strict + error
    testSqlApi("json_exists(f9, 'strict $.info.type1' false on error)", "false")
    testSqlApi("json_exists(f9, 'strict $.info.type1' true on error)", "true")
    testSqlApi("json_exists(f9, 'strict $.info.type' true on error)", "true")
    testSqlApi("json_exists(f9, 'strict $.info.type' false on error)", "true")
    testSqlApi("json_exists(f9, 'strict $.info.type' unknown on error)", "true")
    testSqlApi("json_exists(f9, 'strict $.info.\"type\"' unknown on error)", "null")
    testSqlApi("json_exists(f9, 'strict $.info.type' error on error)", "true")

    verifyJsonExistsException("json_exists(f7, 'lax aa')",
      "Illegal Parameter Type error : JSON_EXISTS(INT, CHAR(6) NOT NULL)")
    verifyJsonExistsException("json_exists(f9, 'lax $' error on error)",
      "Illegal operation, JSON_EXISTS no need to fill in the error behavior in lax mode!")
    verifyJsonExistsException("json_exists(f9, '$.info.type')",
      "Illegal jsonpath spec: $.info.type, format of the spec should be: '<lax|strict> ${expr}'")
  }

  private def verifyJsonExistsException[T <: Exception](
       sqlExpr: String,
       expectedMessage: String
     ): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(startsWith(expectedMessage))
    testSqlApi(sqlExpr, "null")
    expectedException = ExpectedException.none()
  }
}

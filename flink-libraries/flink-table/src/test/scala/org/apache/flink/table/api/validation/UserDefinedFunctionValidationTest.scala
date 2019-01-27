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
package org.apache.flink.table.api.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.expressions.utils.{Func0, Func9}
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.OverAgg0
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class UserDefinedFunctionValidationTest extends TableTestBase {

  @Test
  def testScalarFunctionOperandTypeCheck(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(TableErrors.prettyPrint(
      "SQL validation failed:\nGiven parameters of function 'func' do not match any " +
        "signature. \n" +
        "Actual: (java.lang.String) \n" +
        "Expected: (int)"))
    val util = streamTestUtil()
    util.addTable[(Int, String)]("t", 'a, 'b)
    util.addFunction("func", Func0)
    util.explainSql("select func(b) from t")
  }

  @Test
  def testScalarFunctionUnmatch(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(TableErrors.prettyPrint(
      "SQL validation failed:\nGiven parameters of function 'func' do not match any " +
        "signature. \n" +
        "Actual: (java.lang.Integer) \n" +
        "Expected: (int, int, long)"))
    val util = streamTestUtil()
    util.addTable[(Int, String)]("t", 'a, 'b)
    util.addFunction("func", Func9)
    util.explainSql("select func(a) from t")
  }

  @Test
  def testAggregateFunctionOperandTypeCheck(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      TableErrors.prettyPrint(
        "SQL validation failed:\nGiven parameters of function 'agg' do not match any" +
          " signature. \nActual: (java.lang.String, java.lang.Integer) \n" +
          "Expected: (org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions" +
          ".Accumulator0, long, int)"))

    val util = streamTestUtil()
    util.addTable[(Int, String)]("t", 'a, 'b)
    util.addFunction("agg", new OverAgg0)
    util.explainSql("select agg(b, a) from t")
  }

}


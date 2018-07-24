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
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.{Func0, Func21, Func22}
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.OverAgg0
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class UserDefinedFunctionValidationTest extends TableTestBase {

  @Test
  def testScalarSqlFunctionOperandTypeCheck(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Given parameters of function 'func' do not match any signature. \n" +
        "Actual: (java.lang.String) \n" +
        "Expected: (int)")
    val util = streamTestUtil()
    util.addTable[(Int, String)]("t", 'a, 'b)
    util.tableEnv.registerFunction("func", Func0)
    util.verifySql("select func(b) from t", "n/a")
  }

  @Test
  def testScalarTableFunctionOperandTypeCheck(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Expression org.apache.flink.table.expressions.utils.Func0$('b) " +
        "failed on input check: Given parameters do not match any signature. \n" +
        "Actual: (java.lang.String) \n" +
        "Expected: (int)")
    val util = streamTestUtil()
    val sourceTable = util.addTable[(Int, String)]("t", 'a, 'b)
    val resultTable = sourceTable.select(Func0('b))
    util.verifyTable(resultTable, "n/a")
  }

  @Test
  def testAggregateSqlFunctionOperandTypeCheck(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Given parameters of function do not match any signature. \n" +
        "Actual: (java.lang.String, java.lang.Integer) \n" +
        "Expected: (org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions" +
        ".Accumulator0, long, int)")

    val util = streamTestUtil()
    val agg = new OverAgg0
    util.addTable[(Int, String)]("t", 'a, 'b)
    util.tableEnv.registerFunction("agg", agg)
    util.verifySql("select agg(b, a) from t", "n/a")
  }

  @Test
  def testAggregateTableFunctionOperandTypeCheck(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Expression OverAgg0(ArrayBuffer('b, 'a)) failed on input check: " +
        "Given parameters do not match any signature. \n" +
        "Actual: (java.lang.String, java.lang.Integer) \n" +
        "Expected: (long, int)")
    val util = streamTestUtil()
    val agg = new OverAgg0
    val sourceTable = util.addTable[(Int, String)]("t", 'a, 'b)
    val resultTable = sourceTable.select(agg('b, 'a))
    util.verifyTable(resultTable, "n/a")
  }

  @Test
  def userDefineMapTypeFunctionSubSchemaTypeCheck(): Unit = {
    thrown.expect(classOf[ValidationException])
    val util = streamTestUtil()
    util.addTable("t", 'a)(Types.ROW(Types.MAP(Types.INT, Types.INT)))
    util.tableEnv.registerFunction("func", Func21)
    util.verifySql("select func(a) from t", "n/a")
  }

  @Test
  def userDefineRowTypeFunctionSubSchemaTypeCheck(): Unit = {
    thrown.expect(classOf[ValidationException])
    val util = streamTestUtil()
    util.addTable("t", 'a)(Types.ROW(Types.ROW(Types.STRING, Types.INT)))
    util.tableEnv.registerFunction("func", Func22)
    util.verifySql("select func(a) from t", "n/a")
  }
}


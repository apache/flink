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
package org.apache.flink.table.api.stream.table.validation

import java.math.BigDecimal

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, Tumble, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.utils.{TableFunc0, TableTestBase}
import org.junit.Test

class CalcValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidUseOfRowtime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, Double, Float, BigDecimal, String)](
      "MyTable",
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    .select('rowtime.rowtime)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidUseOfRowtime2(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, Double, Float, BigDecimal, String)](
      "MyTable",
      'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    .window(Tumble over 2.millis on 'rowtime as 'w)
    .groupBy('w)
    .select('w.end.rowtime, 'int.count as 'int) // no rowtime on non-window reference
  }

  @Test(expected = classOf[ValidationException])
  def testAddColumnsWithAgg(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.addColumns('a.sum)
  }

  @Test(expected = classOf[ValidationException])
  def testAddOrReplaceColumnsWithAgg(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.addOrReplaceColumns('a.sum)
  }

  @Test(expected = classOf[ValidationException])
  def testRenameColumnsWithAgg(): Unit = {
      val util = streamTestUtil()
      val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
      tab.renameColumns('a.sum)
  }

  @Test(expected = classOf[ValidationException])
  def testRenameColumnsWithoutAlias(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.renameColumns('a)
  }

  @Test(expected = classOf[ValidationException])
  def testRenameColumnsWithFunctallCall(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.renameColumns('a + 1  as 'a2)
  }

  @Test(expected = classOf[ValidationException])
  def testRenameColumnsNotExist(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.renameColumns('e as 'e2)
  }

  @Test(expected = classOf[ValidationException])
  def testDropColumnsWithAgg(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.dropColumns('a.sum)
  }

  @Test(expected = classOf[ValidationException])
  def testDropColumnsNotExist(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.dropColumns('e)
  }

  @Test(expected = classOf[ValidationException])
  def testDropColumnsWithValueLiteral(): Unit = {
    val util = streamTestUtil()
    val tab = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    tab.dropColumns("'a'")
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMapFunctionTypeAggregation(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int)](
      "MyTable", 'int)
      .map('int.sum) // do not support AggregateFunction as input
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMapFunctionTypeUDAGG(): Unit = {
    val util = streamTestUtil()

    val weightedAvg = new WeightedAvg
    util.addTable[(Int)](
      "MyTable", 'int)
      .map(weightedAvg('int, 'int)) // do not support AggregateFunction as input
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMapFunctionTypeUDAGG2(): Unit = {
    val util = streamTestUtil()

    util.tableEnv.registerFunction("weightedAvg", new WeightedAvg)
    util.addTable[(Int)](
      "MyTable", 'int)
      .map(call("weightedAvg", $"int", $"int")) // do not support AggregateFunction as input
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMapFunctionTypeTableFunction(): Unit = {
    val util = streamTestUtil()

    util.tableEnv.registerFunction("func", new TableFunc0)
    util.addTable[(String)](
      "MyTable", 'string)
      .map(call("func", $"string") as "a") // do not support TableFunction as input
  }
}

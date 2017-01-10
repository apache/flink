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

package org.apache.flink.table.api.scala.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.junit._

class AggregationsValidationTest {

  @Test(expected = classOf[ValidationException])
  def testNonWorkingAggregationDataTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    env.fromElements(("Hello", 1)).toTable(tEnv)
      // Must fail. Field '_1 is not a numeric type.
      .select('_1.sum)
  }

  @Test(expected = classOf[ValidationException])
  def testNoNestedAggregations(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    env.fromElements(("Hello", 1)).toTable(tEnv)
      // Must fail. Sum aggregation can not be chained.
      .select('_2.sum.sum)
  }

  @Test(expected = classOf[ValidationException])
  def testGroupingOnNonExistentField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      // must fail. '_foo not a valid field
      .groupBy('_foo)
      .select('a.avg)
  }

  @Test(expected = classOf[ValidationException])
  def testGroupingInvalidSelection(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('a, 'b)
      // must fail. 'c is not a grouping key or aggregation
      .select('c)
  }

  @Test(expected = classOf[ValidationException])
  def testAggregationOnNonExistingField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
      // Must fail. Field 'foo does not exist.
      .select('foo.avg)
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testAggregationOnNonExistingFieldJava() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val table = CollectionDataSets.get3TupleDataSet(env).toTable(tableEnv)
    table.select("foo.avg")
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testNonWorkingAggregationDataTypesJava() {
    val env= ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val table = env.fromElements((1f, "Hello")).toTable(tableEnv)
    // Must fail. Cannot compute SUM aggregate on String field.
    table.select("f1.sum")
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testNoNestedAggregationsJava() {
    val env= ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val table = env.fromElements((1f, "Hello")).toTable(tableEnv)
    // Must fail. Aggregation on aggregation not allowed.
    table.select("f0.sum.sum")
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testGroupingOnNonExistentFieldJava() {
    val env= ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val input = CollectionDataSets.get3TupleDataSet(env).toTable(tableEnv, 'a, 'b, 'c)
    input
      // must fail. Field foo is not in input
      .groupBy("foo")
      .select("a.avg")
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testGroupingInvalidSelectionJava() {
    val env= ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val input = CollectionDataSets.get3TupleDataSet(env).toTable(tableEnv, 'a, 'b, 'c)
    input
      .groupBy("a, b")
      // must fail. Field c is not a grouping key or aggregation
      .select("c")
  }

}

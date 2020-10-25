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

package org.apache.flink.table.api.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMergeAndReset
import org.apache.flink.table.utils.TableTestBase

import org.junit._

class AggregateValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testNonWorkingAggregationDataTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(String, Int)]("Table2")

    // Must fail. Field '_1 is not a numeric type.
    t.select('_1.sum)
  }

  @Test(expected = classOf[ValidationException])
  def testNoNestedAggregations(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(String, Int)]("Table2")

    // Must fail. Sum aggregation can not be chained.
    t.select('_2.sum.sum)
  }

  @Test(expected = classOf[ValidationException])
  def testGroupingOnNonExistentField(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. '_foo not a valid field
    t.groupBy('_foo).select('a.avg)
  }

  @Test(expected = classOf[ValidationException])
  def testGroupingInvalidSelection(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    t.groupBy('a, 'b)
    // must fail. 'c is not a grouping key or aggregation
    .select('c)
  }

  @Test(expected = classOf[ValidationException])
  def testAggregationOnNonExistingField(): Unit = {

    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // Must fail. Field 'foo does not exist.
    t.select('foo.avg)
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testInvalidUdAggArgs() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset

    // must fail. UDAGG does not accept String type
    t.select(myWeightedAvg('c, 'a))
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testGroupingInvalidUdAggArgs() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset

    t.groupBy('b)
    // must fail. UDAGG does not accept String type
    .select(myWeightedAvg('c, 'a))
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testGroupingNestedUdAgg() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset

    t.groupBy('c)
    // must fail. UDAGG does not accept String type
    .select(myWeightedAvg(myWeightedAvg('b, 'a), 'a))
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testAggregationOnNonExistingFieldJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    t.select($"foo".avg)
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testNonWorkingAggregationDataTypesJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Long, String)]("Table2",'b, 'c)
    // Must fail. Cannot compute SUM aggregate on String field.
    t.select($"c".sum)
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testNoNestedAggregationsJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Long, String)]("Table2",'b, 'c)
    // Must fail. Aggregation on aggregation not allowed.
    t.select($"b".sum.sum)
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testNoDeeplyNestedAggregationsJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Long, String)]("Table2",'b, 'c)
    // Must fail. Aggregation on aggregation not allowed.
    t.select(($"b".sum + 1).sum)
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testGroupingOnNonExistentFieldJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. Field foo is not in input
    t.groupBy($"foo")
    .select($"a".avg)
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testGroupingInvalidSelectionJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    t.groupBy($"a", $"b")
    // must fail. Field c is not a grouping key or aggregation
    .select($"c")
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testUnknownUdAggJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. unknown is not known
    .select(call("unknown", $"c"))
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testGroupingUnknownUdAggJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    t.groupBy($"a", $"b")
    // must fail. unknown is not known
    .select(call("unknown", $"c"))
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testInvalidUdAggArgsJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset
    util.tableEnv.registerFunction("myWeightedAvg", myWeightedAvg)

    // must fail. UDAGG does not accept String type
    t.select(call("myWeightedAvg", $"c", $"a"))
  }

  @Test(expected = classOf[ValidationException])
  @throws[Exception]
  def testGroupingInvalidUdAggArgsJava() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset
    util.tableEnv.registerFunction("myWeightedAvg", myWeightedAvg)

    t.groupBy($"b")
    // must fail. UDAGG does not accept String type
    t.select(call("myWeightedAvg", $"c", $"a"))
  }
}

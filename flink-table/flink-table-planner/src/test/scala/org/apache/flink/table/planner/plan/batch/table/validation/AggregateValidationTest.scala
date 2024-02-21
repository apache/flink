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
package org.apache.flink.table.planner.plan.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMergeAndReset
import org.apache.flink.table.planner.utils.TableTestBase

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class AggregateValidationTest extends TableTestBase {

  @Test
  def testNonWorkingAggregationDataTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(String, Int)]("Table2")

    // Must fail. Field '_1 is not a numeric type.
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select('_1.sum))
  }

  @Test
  def testNoNestedAggregations(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(String, Int)]("Table2")

    // Must fail. Sum aggregation can not be chained.
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select('_2.sum.sum))
  }

  @Test
  def testGroupingOnNonExistentField(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. '_foo not a valid field
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.groupBy('_foo).select('a.avg))
  }

  @Test
  def testGroupingInvalidSelection(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          t.groupBy('a, 'b)
            // must fail. 'c is not a grouping key or aggregation
            .select('c))
  }

  @Test
  def testAggregationOnNonExistingField(): Unit = {

    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // Must fail. Field 'foo does not exist.
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select('foo.avg))
  }

  @Test
  def testInvalidUdAggArgs() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset

    // must fail. UDAGG does not accept String type
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select(myWeightedAvg('c, 'a)))
  }

  @Test
  def testGroupingInvalidUdAggArgs() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          t.groupBy('b)
            // must fail. UDAGG does not accept String type
            .select(myWeightedAvg('c, 'a)))
  }

  @Test
  def testGroupingNestedUdAgg() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          t.groupBy('c)
            // must fail. UDAGG does not accept String type
            .select(myWeightedAvg(myWeightedAvg('b, 'a), 'a)))
  }

  @Test
  def testAggregationOnNonExistingFieldJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select($"foo".avg))
  }

  @Test
  def testNonWorkingAggregationDataTypesJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Long, String)]("Table2", 'b, 'c)
    // Must fail. Cannot compute SUM aggregate on String field.
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select($"c".sum))
  }

  @Test
  def testNoNestedAggregationsJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Long, String)]("Table2", 'b, 'c)
    // Must fail. Aggregation on aggregation not allowed.
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select($"b".sum.sum))
  }

  @Test
  def testNoDeeplyNestedAggregationsJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Long, String)]("Table2", 'b, 'c)
    // Must fail. Aggregation on aggregation not allowed.
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select(($"b".sum + 1).sum))
  }

  @Test
  @throws[Exception]
  def testGroupingOnNonExistentFieldJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. Field foo is not in input
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.groupBy($"foo").select($"a".avg))
  }

  @Test
  def testGroupingInvalidSelectionJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          t.groupBy($"a", $"b")
            // must fail. Field c is not a grouping key or aggregation
            .select($"c"))
  }

  @Test
  def testUnknownUdAggJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. unknown is not known
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select(call("unknown", $"c")))
  }

  @Test
  def testGroupingUnknownUdAggJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          t.groupBy($"a", $"b")
            // must fail. unknown is not known
            .select(call("unknown", $"c")))
  }

  @Test
  def testInvalidUdAggArgsJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset
    util.addTemporarySystemFunction("myWeightedAvg", myWeightedAvg)

    // must fail. UDAGG does not accept String type
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select(call("myWeightedAvg", $"c", $"a")))
  }

  @Test
  def testGroupingInvalidUdAggArgsJava() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset
    util.addTemporarySystemFunction("myWeightedAvg", myWeightedAvg)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          t.groupBy($"b")
            // must fail. UDAGG does not accept String type
            .select(call("myWeightedAvg", $"c", $"a")))
  }
}

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

  @Test
  def testNonWorkingAggregationDataTypes(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(String, Int)]("Table2")

    // Must fail. Field '_1 is not a numeric type.
    t.select('_1.sum)
        }
    }

  @Test
  def testNoNestedAggregations(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(String, Int)]("Table2")

    // Must fail. Sum aggregation can not be chained.
    t.select('_2.sum.sum)
        }
    }

  @Test
  def testGroupingOnNonExistentField(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. '_foo not a valid field
    t.groupBy('_foo).select('a.avg)
        }
    }

  @Test
  def testGroupingInvalidSelection(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    t.groupBy('a, 'b)
    // must fail. 'c is not a grouping key or aggregation
    .select('c)
        }
    }

  @Test
  def testAggregationOnNonExistingField(): Unit = {
        assertThrows[ValidationException] {

    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // Must fail. Field 'foo does not exist.
    t.select('foo.avg)
        }
    }

  @Test
  @throws[Exception]
  def testInvalidUdAggArgs() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset

    // must fail. UDAGG does not accept String type
    t.select(myWeightedAvg('c, 'a))
        }
    }

  @Test
  @throws[Exception]
  def testGroupingInvalidUdAggArgs() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset

    t.groupBy('b)
    // must fail. UDAGG does not accept String type
    .select(myWeightedAvg('c, 'a))
        }
    }

  @Test
  @throws[Exception]
  def testGroupingNestedUdAgg() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset

    t.groupBy('c)
    // must fail. UDAGG does not accept String type
    .select(myWeightedAvg(myWeightedAvg('b, 'a), 'a))
        }
    }

  @Test
  @throws[Exception]
  def testAggregationOnNonExistingFieldJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    t.select($"foo".avg)
        }
    }

  @Test
  @throws[Exception]
  def testNonWorkingAggregationDataTypesJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Long, String)]("Table2",'b, 'c)
    // Must fail. Cannot compute SUM aggregate on String field.
    t.select($"c".sum)
        }
    }

  @Test
  @throws[Exception]
  def testNoNestedAggregationsJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Long, String)]("Table2",'b, 'c)
    // Must fail. Aggregation on aggregation not allowed.
    t.select($"b".sum.sum)
        }
    }

  @Test
  @throws[Exception]
  def testNoDeeplyNestedAggregationsJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Long, String)]("Table2",'b, 'c)
    // Must fail. Aggregation on aggregation not allowed.
    t.select(($"b".sum + 1).sum)
        }
    }

  @Test
  @throws[Exception]
  def testGroupingOnNonExistentFieldJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. Field foo is not in input
    t.groupBy($"foo")
    .select($"a".avg)
        }
    }

  @Test
  @throws[Exception]
  def testGroupingInvalidSelectionJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    t.groupBy($"a", $"b")
    // must fail. Field c is not a grouping key or aggregation
    .select($"c")
        }
    }

  @Test
  @throws[Exception]
  def testUnknownUdAggJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. unknown is not known
    .select(call("unknown", $"c"))
        }
    }

  @Test
  @throws[Exception]
  def testGroupingUnknownUdAggJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    t.groupBy($"a", $"b")
    // must fail. unknown is not known
    .select(call("unknown", $"c"))
        }
    }

  @Test
  @throws[Exception]
  def testInvalidUdAggArgsJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset
    util.tableEnv.registerFunction("myWeightedAvg", myWeightedAvg)

    // must fail. UDAGG does not accept String type
    t.select(call("myWeightedAvg", $"c", $"a"))
        }
    }

  @Test
  @throws[Exception]
  def testGroupingInvalidUdAggArgsJava() {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myWeightedAvg = new WeightedAvgWithMergeAndReset
    util.tableEnv.registerFunction("myWeightedAvg", myWeightedAvg)

    t.groupBy($"b")
    // must fail. UDAGG does not accept String type
    t.select(call("myWeightedAvg", $"c", $"a"))
        }
    }
}

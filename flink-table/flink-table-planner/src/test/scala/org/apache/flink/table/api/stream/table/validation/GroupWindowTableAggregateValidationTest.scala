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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Tumble, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.utils.{TableTestBase, Top3}
import org.junit.Test

class GroupWindowTableAggregateValidationTest extends TableTestBase {

  val top3 = new Top3
  val weightedAvg = new WeightedAvgWithMerge

  val util = streamTestUtil()
  val table = util.addTable[(Long, Int, String)](
    'long, 'int, 'string, 'rowtime.rowtime, 'proctime.proctime)

  @Test
  def testInvalidArgs(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters do not match any signature. \n" +
      "Actual: (java.lang.Long) \nExpected: (int)")

    table
      .window(Tumble over 2.hours on 'rowtime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('long)) // invalid args
      .select('string, 'f0)
  }

  @Test
  def testInvalidAggregateInSelection(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Aggregate functions cannot be used in the select " +
      "right after the flatAggregate.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('string, 'f0.count)
  }
}

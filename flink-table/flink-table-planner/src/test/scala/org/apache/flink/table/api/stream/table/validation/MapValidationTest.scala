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
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.utils.{TableFunc0, TableTestBase}
import org.junit.Test

class MapValidationTest extends TableTestBase {

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
      .map("weightedAvg(int, int)") // do not support AggregateFunction as input
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMapFunctionTypeTableFunction(): Unit = {
    val util = streamTestUtil()

    util.tableEnv.registerFunction("func", new TableFunc0)
    util.addTable[(String)](
      "MyTable", 'string)
      .map("func(string) as a") // do not support TableFunction as input
  }
}

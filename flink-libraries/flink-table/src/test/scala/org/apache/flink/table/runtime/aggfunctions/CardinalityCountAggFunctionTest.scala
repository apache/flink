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
package org.apache.flink.table.runtime.aggfunctions

import java.lang.reflect.Method

import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.runtime.functions.aggfunctions.CardinalityCountAggFunction
import org.apache.flink.table.runtime.functions.aggfunctions.cardinality.CardinalityCountAccumulator

class CardinalityCountAggFunctionTest
  extends AggFunctionTestBase[Long, CardinalityCountAccumulator] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      List(0.01, 1),
      List(0.01, 2),
      List(0.01, 3),
      List(0.01, 4),
      List(0.01, 5),
      List(0.01, 6),
      List(0.01, 7),
      List(0.01, 7),
      List(0.01, 7)),
    Seq(
      List(0.01, "a"),
      List(0.01, "b"),
      List(0.01, "c"),
      List(0.01, "2"),
      List(0.01, "2"))
  )

  override def ignoreMethods = List[String]("retract")

  override def accumulateFunc: Method =
    aggregator.getClass.getMethod("accumulate", accType, classOf[java.lang.Double], classOf[Any])

  override def accumulateVals(vals: Seq[_]): CardinalityCountAccumulator = {
    val accumulator = aggregator.createAccumulator()
    vals.foreach(
      v =>
        accumulateFunc.invoke(
          aggregator,
          accumulator.asInstanceOf[Object],
          v.asInstanceOf[List[_]](0).asInstanceOf[java.lang.Double],
          v.asInstanceOf[List[_]](1).asInstanceOf[Object])
    )
    accumulator
  }
  override def expectedResults: Seq[Long] = Seq(7, 4)

  override def aggregator: AggregateFunction[Long, CardinalityCountAccumulator] = {
    new CardinalityCountAggFunction()
    .asInstanceOf[AggregateFunction[Long, CardinalityCountAccumulator]]
  }
}

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

import java.lang.{Long => JLong}

import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions.{CountAccumulator, CountAggFunction}

/**
  * Test case for built-in count aggregate function
  */
class CountAggFunctionTest extends AggFunctionTestBase[JLong, CountAccumulator] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq("a", "b", null, "c", null, "d", "e", null, "f"),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[JLong] = Seq(6L, 0L)

  override def aggregator: AggregateFunction[JLong, CountAccumulator] = new CountAggFunction

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

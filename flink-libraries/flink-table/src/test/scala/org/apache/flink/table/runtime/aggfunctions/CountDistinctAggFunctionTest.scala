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

import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.runtime.functions.aggfunctions.CountDistinct.LongCountDistinctAggFunction
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Test case for built-in count distinct aggregate function
  */
class CountDistinctAggFunctionTest extends AggFunctionTestBase[JLong, GenericRow] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1L, 5L, null, 5L, null, 7L, 7L, null, 7L),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[JLong] = Seq(3L, 0L)

  override def aggregator: AggregateFunction[JLong, GenericRow] = new LongCountDistinctAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])

  @Test
  // test for verifying bug fixed in the case that a retract followed by a accumulate
  def testCountDistinctAggFunction(): Unit = {
    val acc = aggregator.createAccumulator()

    aggregator.asInstanceOf[LongCountDistinctAggFunction].retract(acc, 5L)
    aggregator.asInstanceOf[LongCountDistinctAggFunction].accumulate(acc, 5L)
    assertEquals(0L, aggregator.getValue(acc))
  }
}

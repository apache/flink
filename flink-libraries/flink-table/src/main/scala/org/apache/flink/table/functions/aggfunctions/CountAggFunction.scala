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
package org.apache.flink.table.functions.aggfunctions

import java.util.{List => JList}
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/**
  * built-in count aggregate function
  */
class CountAggFunction extends AggregateFunction[Long] {

  /** The initial accumulator for count aggregate function */
  class CountAccumulator extends JTuple1[Long] with Accumulator {
    f0 = 0 //count
  }

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      accumulator.asInstanceOf[CountAccumulator].f0 += 1
    }
  }

  override def getValue(accumulator: Accumulator): Long = {
    accumulator.asInstanceOf[CountAccumulator].f0
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[CountAccumulator]
    var i: Int = 1
    while (i < accumulators.size()) {
      ret.f0 += accumulators.get(i).asInstanceOf[CountAccumulator].f0
      i += 1
    }
    ret
  }

  override def createAccumulator(): Accumulator = {
    new CountAccumulator
  }
}

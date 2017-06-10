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

import java.lang.{Iterable => JIterable}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for count aggregate function */
class CountAccumulator extends JTuple1[Long] {
  f0 = 0L //count
}

/**
  * built-in count aggregate function
  */
class CountAggFunction extends AggregateFunction[Long, CountAccumulator] {

  def accumulate(acc: CountAccumulator, value: Any): Unit = {
    if (value != null) {
      acc.f0 += 1L
    }
  }

  def retract(acc: CountAccumulator, value: Any): Unit = {
    if (value != null) {
      acc.f0 -= 1L
    }
  }

  override def getValue(acc: CountAccumulator): Long = {
    acc.f0
  }

  def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0 += iter.next().f0
    }
  }

  override def createAccumulator(): CountAccumulator = {
    new CountAccumulator
  }

  def resetAccumulator(acc: CountAccumulator): Unit = {
    acc.f0 = 0L
  }

  def getAccumulatorType(): TypeInformation[_] = {
    new TupleTypeInfo((new CountAccumulator).getClass, BasicTypeInfo.LONG_TYPE_INFO)
  }

  def getResultType(): TypeInformation[_] =
    BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[_]]
}

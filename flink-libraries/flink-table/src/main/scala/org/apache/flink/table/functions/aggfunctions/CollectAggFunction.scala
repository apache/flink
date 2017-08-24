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

import org.apache.commons.collections4.multiset.{AbstractMultiSet, HashMultiSet}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, TupleTypeInfo}
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for Collect aggregate function */
class CollectAccumulator[E] extends JTuple1[AbstractMultiSet[E]]

abstract class CollectAggFunction[E]
  extends AggregateFunction[AbstractMultiSet[E], CollectAccumulator[E]] {

  override def createAccumulator(): CollectAccumulator[E] = {
    val acc = new CollectAccumulator[E]()
    acc.f0 = new HashMultiSet()
    acc
  }

  def accumulate(accumulator: CollectAccumulator[E], value: E): Unit = {
    if (value != null) {
      accumulator.f0.add(value)
    }
  }

  override def getValue(accumulator: CollectAccumulator[E]): AbstractMultiSet[E] = {
    if (accumulator.f0.size() > 0) {
      new HashMultiSet(accumulator.f0)
    } else {
      null.asInstanceOf[AbstractMultiSet[E]]
    }
  }

  def resetAccumulator(acc: CollectAccumulator[E]): Unit = {
    acc.f0.clear()
  }

  override def getAccumulatorType: TypeInformation[CollectAccumulator[E]] = {
    new TupleTypeInfo(
      classOf[CollectAccumulator[E]],
      new GenericTypeInfo[AbstractMultiSet[E]](classOf[AbstractMultiSet[E]]))
  }

  def merge(acc: CollectAccumulator[E], its: JIterable[CollectAccumulator[E]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0.addAll(iter.next().f0)
    }
  }

  def retract(acc: CollectAccumulator[E], value: Any): Unit = {
    if (value != null) {
      acc.f0.remove(value)
    }
  }
}

class IntCollectAggFunction extends CollectAggFunction[Int] {
}

class LongCollectAggFunction extends CollectAggFunction[Long] {
}

class StringCollectAggFunction extends CollectAggFunction[String] {
}

class ByteCollectAggFunction extends CollectAggFunction[Byte] {
}

class ShortCollectAggFunction extends CollectAggFunction[Short] {
}

class FloatCollectAggFunction extends CollectAggFunction[Float] {
}

class DoubleCollectAggFunction extends CollectAggFunction[Double] {
}

class BooleanCollectAggFunction extends CollectAggFunction[Boolean] {
}

class ObjectCollectAggFunction extends CollectAggFunction[Object] {
}

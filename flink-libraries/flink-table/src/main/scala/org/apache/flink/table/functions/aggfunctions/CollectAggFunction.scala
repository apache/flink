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
import java.util
import java.util.function.BiFunction

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, TupleTypeInfo}
import org.apache.flink.table.functions.AggregateFunction

import scala.collection.JavaConverters._

/** The initial accumulator for Collect aggregate function */
class CollectAccumulator[E] extends JTuple1[util.Map[E, Integer]]

abstract class CollectAggFunction[E]
  extends AggregateFunction[util.Map[E, Integer], CollectAccumulator[E]] {

  @transient
  private lazy val addFunction = new BiFunction[Integer, Integer, Integer] {
    override def apply(t: Integer, u: Integer): Integer = t + u
  }

  override def createAccumulator(): CollectAccumulator[E] = {
    val acc = new CollectAccumulator[E]()
    acc.f0 = new util.HashMap[E, Integer]()
    acc
  }

  def accumulate(accumulator: CollectAccumulator[E], value: E): Unit = {
    if (value != null) {
      if (accumulator.f0.containsKey(value)) {
        val add = (x: Integer, y: Integer) => x + y
        accumulator.f0.merge(value, 1, addFunction)
      } else {
        accumulator.f0.put(value, 1)
      }
    }
  }

  override def getValue(accumulator: CollectAccumulator[E]): util.Map[E, Integer] = {
    if (accumulator.f0.size() > 0) {
      new util.HashMap(accumulator.f0)
    } else {
      null.asInstanceOf[util.Map[E, Integer]]
    }
  }

  def resetAccumulator(acc: CollectAccumulator[E]): Unit = {
    acc.f0.clear()
  }

  override def getAccumulatorType: TypeInformation[CollectAccumulator[E]] = {
    new TupleTypeInfo(
      classOf[CollectAccumulator[E]],
      new GenericTypeInfo[util.Map[E, Integer]](classOf[util.Map[E, Integer]]))
  }

  def merge(acc: CollectAccumulator[E], its: JIterable[CollectAccumulator[E]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      for ((k: E, v: Integer) <- iter.next().f0.asScala) {
        acc.f0.merge(k, v, addFunction)
      }
    }
  }

  def retract(acc: CollectAccumulator[E], value: E): Unit = {
    if (value != null) {
      if (0 == acc.f0.merge(value, -1, addFunction)) {
        acc.f0.remove(value)
      }
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

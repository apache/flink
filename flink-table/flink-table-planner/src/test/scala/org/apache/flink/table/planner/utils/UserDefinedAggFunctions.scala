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

package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{ObjectArrayTypeInfo, RowTypeInfo, TupleTypeInfo}
import org.apache.flink.table.annotation.{DataTypeHint, InputGroup}
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

import java.lang.{Float => JFloat, Integer => JInt, Long => JLong}
import java.util

/**
  * User-defined aggregation function to compute the top 10 most visited Int IDs
  * with the highest Float values. We use an Array[Tuple2[Int, Float]] as accumulator to
  * store the top 10 entries.
  *
  * The result is emitted as Array as well.
  */
class Top10 extends AggregateFunction[Array[JTuple2[JInt, JFloat]], Array[JTuple2[JInt, JFloat]]] {

  @Override
  def createAccumulator(): Array[JTuple2[JInt, JFloat]] = {
    new Array[JTuple2[JInt, JFloat]](10)
  }

  /**
    * Adds a new entry and count to the top 10 entries if necessary.
    *
    * @param acc   The current top 10
    * @param id    The ID
    * @param value The value for the ID
    */
  def accumulate(acc: Array[JTuple2[JInt, JFloat]], id: Int, value: Float) {

    var i = 9
    var skipped = 0

    // skip positions without records
    while (i >= 0 && acc(i) == null) {
      if (acc(i) == null) {
        // continue until first entry in the top10 list
        i -= 1
      }
    }
    // backward linear search for insert position
    while (i >= 0 && value > acc(i).f1) {
      // check next entry
      skipped += 1
      i -= 1
    }

    // set if necessary
    if (i < 9) {
      // move entries with lower count by one position
      if (i < 8 && skipped > 0) {
        System.arraycopy(acc, i + 1, acc, i + 2, skipped)
      }

      // add ID to top10 list
      acc(i + 1) = JTuple2.of(id, value)
    }
  }

  override def getValue(acc: Array[JTuple2[JInt, JFloat]]): Array[JTuple2[JInt, JFloat]] = acc

  def merge(
      acc: Array[JTuple2[JInt, JFloat]],
      its: java.lang.Iterable[Array[JTuple2[JInt, JFloat]]]): Unit = {

    val it = its.iterator()
    while (it.hasNext) {
      val acc2 = it.next()

      var i = 0
      var i2 = 0
      while (i < 10 && i2 < 10 && acc2(i2) != null) {
        if (acc(i) == null) {
          // copy to empty place
          acc(i) = acc2(i2)
          i += 1
          i2 += 1
        } else if (acc(i).f1.asInstanceOf[Float] >= acc2(i2).f1.asInstanceOf[Float]) {
          // forward to next
          i += 1
        } else {
          // shift and copy
          System.arraycopy(acc, i, acc, i + 1, 9 - i)
          acc(i) = acc2(i2)
          i += 1
          i2 += 1
        }
      }
    }
  }

  override def getAccumulatorType: TypeInformation[Array[JTuple2[JInt, JFloat]]] = {
    ObjectArrayTypeInfo.getInfoFor(new TupleTypeInfo[JTuple2[JInt, JFloat]](Types.INT, Types.FLOAT))
  }

  override def getResultType: TypeInformation[Array[JTuple2[JInt, JFloat]]] = {
    ObjectArrayTypeInfo.getInfoFor(new TupleTypeInfo[JTuple2[JInt, JFloat]](Types.INT, Types.FLOAT))
  }
}

case class NonMergableCountAcc(var count: Long)

class NonMergableCount extends AggregateFunction[java.lang.Long, NonMergableCountAcc] {

  def accumulate(
      acc: NonMergableCountAcc,
      @DataTypeHint(inputGroup = InputGroup.ANY) value: Any): Unit = {
    if (null != value) {
      acc.count = acc.count + 1
    }
  }

  override def createAccumulator(): NonMergableCountAcc = NonMergableCountAcc(0)

  override def getValue(acc: NonMergableCountAcc): java.lang.Long = acc.count
}

case class CountMinMaxAcc(var count: Long, var min: Int, var max: Int)

@DataTypeHint("ROW<f0 BIGINT, f1 INT, f2 INT>")
class CountMinMax extends AggregateFunction[Row, CountMinMaxAcc] {

  def accumulate(acc: CountMinMaxAcc, value: java.lang.Integer): Unit = {
    if (acc.count == 0 || value < acc.min) {
      acc.min = value
    }
    if (acc.count == 0 || value > acc.max) {
      acc.max = value
    }
    acc.count += 1
  }

  override def createAccumulator(): CountMinMaxAcc = CountMinMaxAcc(0L, 0, 0)

  override def getValue(acc: CountMinMaxAcc): Row = {
    val min: Int = if (acc.count > 0) {
      acc.min
    } else {
      null.asInstanceOf[Int]
    }

    val max: Int = if (acc.count > 0) {
      acc.max
    } else {
      null.asInstanceOf[Int]
    }
    Row.of(JLong.valueOf(acc.count), JInt.valueOf(min), JInt.valueOf(max))
  }
}

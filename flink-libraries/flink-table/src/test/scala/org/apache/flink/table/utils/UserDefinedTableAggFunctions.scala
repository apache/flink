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

package org.apache.flink.table.utils

import java.sql.Timestamp
import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.table.api.Types
import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.functions.TableAggregateFunction

import scala.collection.mutable.ArrayBuffer

class TopNAccum {
  var data: MapView[JInt, JLong] = _  // (rank -> value)
  var dataSize: JInt = _
  var min: JLong = _
  var input: ArrayBuffer[(Int, Long)] = _
}

/**
  * User-defined table aggregation function to compute the topN most visited Int IDs
  * with the highest Float values. Note: this function can not handle retract input.
  */
class TopN(n: Int) extends TableAggregateFunction[JTuple3[JInt, JLong, JInt], TopNAccum] {

  override def createAccumulator(): TopNAccum = {
    val acc = new TopNAccum
    acc.data = new MapView(Types.INT, Types.LONG)
    acc.dataSize = 0
    acc.min = JLong.MAX_VALUE
    acc.input = new ArrayBuffer[(Int, Long)]()
    acc
  }

  def accumulate(acc: TopNAccum, category: Int, value: Long) {
    acc.input.append((category, value))
  }

  def emitValue(acc: TopNAccum, out: RetractableCollector[JTuple3[JInt, JLong, JInt]]): Unit = {
    emitValue(acc, out, false)
  }

  def emitValueWithRetract(
    acc: TopNAccum, out: RetractableCollector[JTuple3[JInt, JLong, JInt]]): Unit = {
    emitValue(acc, out, true)
  }

  def emitValue(
    acc: TopNAccum,
    out: RetractableCollector[JTuple3[JInt, JLong, JInt]],
    needEmitWithRetract: Boolean): Unit = {

    for (input <- acc.input) {
      // min >= input, skip directly
      if (acc.dataSize == n && acc.min >= input._2) {
        // skip
      } else {
        if (acc.dataSize == 0) {
          acc.data.put(0, input._2)
          acc.min = input._2
          out.collect(JTuple3.of(input._1, input._2, 0))
        } else {
          var currentRank = acc.dataSize
          // find current rank
          val iterator = acc.data.iterator
          while (iterator.hasNext) {
            val entry = iterator.next()
            if (input._2 > entry.getValue && currentRank > entry.getKey) {
              currentRank = entry.getKey
            }
          }

          var i = acc.dataSize - 1
          while (i >= currentRank) {
            val category = input._1
            val oldRank = i
            val newRank = i + 1
            val v = acc.data.get(oldRank)
            if (needEmitWithRetract) {
              // emit with retract, we need retract old value
              out.retract(JTuple3.of(category, v, oldRank))
            }
            if (newRank < n) {
              acc.data.put(newRank, v)
              if (newRank == n - 1) acc.min = v
              out.collect(JTuple3.of(category, v, newRank))
            }
            i -= 1
          }
          acc.data.put(currentRank, input._2)
          if (currentRank == n - 1) acc.min = input._2
          out.collect(JTuple3.of(input._1, input._2, currentRank))
        }
        if (acc.dataSize < n) acc.dataSize += 1
      }
    }

    // clear input buffer
    acc.input.clear()
  }
}

class Top3Wrapper {
  var top3: ArrayBuffer[Long] = _
  var input: ArrayBuffer[(Int, Long, Boolean)] = _
}

/**
  * User-defined table aggregation function to compute the topN most visited Int IDs with the
  * highest Float values. In order to process retract input, Top3WithRetractInput must store all
  * data, while TopN only store n elements.
  *
  * Note: The performance is bad, only for tests.
  */
class Top3WithRetractInput extends TableAggregateFunction[JTuple3[JInt, JLong, JInt], Top3Wrapper] {

  @Override
  def createAccumulator(): Top3Wrapper = {
    val acc = new Top3Wrapper
    acc.top3 = new ArrayBuffer[Long]()
    acc.input = new ArrayBuffer[(Int, Long, Boolean)]()
    acc
  }

  def accumulate(acc: Top3Wrapper, category: Int, value: Long) {
    acc.input.append((category, value, true))
  }

  def retract(acc: Top3Wrapper, category: Int, value: Long) {
    acc.input.append((category, value, false))
  }

  def emitValue(acc: Top3Wrapper, out: RetractableCollector[JTuple3[JInt, JLong, JInt]]): Unit = {
    emitValue(acc, out, false)
  }

  def emitValueWithRetract(
    acc: Top3Wrapper, out: RetractableCollector[JTuple3[JInt, JLong, JInt]]): Unit = {
    emitValue(acc, out, true)
  }

  def emitValue(
    acc: Top3Wrapper,
    out: RetractableCollector[JTuple3[JInt, JLong, JInt]],
    isRetract: Boolean): Unit = {

    val oldTop = acc.top3
    var newTop = oldTop.clone()
    var category: Int = 0

    for ((cate, v, flag) <- acc.input) {
      category = cate
      // add message
      if (flag) {
        newTop.append(v)
      }
      // delete message
      else {
        val index = newTop.indexOf(v)
        if (index >= 0) {
          newTop.remove(index)
        }
      }
    }

    newTop = newTop.sorted.reverse

    val oldQLength = oldTop.length
    val newQLength = newTop.length
    val minLength = if (oldQLength < newQLength) oldQLength else newQLength

    var i = 0
    while (i < minLength && i < 3) {
      if (oldTop(i) != newTop(i)) {
        if (isRetract) {
          out.retract(JTuple3.of(category, oldTop(i), i))
        }
        out.collect(JTuple3.of(category, newTop(i), i))
      }
      i += 1
    }

    // retract old
    while (i < oldQLength && i < 3) {
      out.retract(JTuple3.of(category, oldTop(i), i))
      i += 1
    }
    // add new
    while (i < newQLength && i < 3) {
      out.collect(JTuple3.of(category, newTop(i), i))
      i += 1
    }

    acc.top3 = newTop
    acc.input.clear()
  }
}

/**
  * Test function for plan test.
  */
class EmptyTableAggFunc extends TableAggregateFunction[(Int, Long, Int), Top3Wrapper] {

  override def createAccumulator(): Top3Wrapper = new Top3Wrapper

  def accumulate(acc: Top3Wrapper, category: Long, value: Timestamp): Unit = {}

  def accumulate(acc: Top3Wrapper, category: Long, value: Int): Unit = {}

  def emitValue(acc: Top3Wrapper, out: RetractableCollector[(Int, Long, Int)]): Unit = {}
}

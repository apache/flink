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

import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import java.lang.{Integer => JInt}
import java.lang.{Iterable => JIterable}
import java.sql.Timestamp
import java.util
import java.util.{Collections, Comparator}

import org.apache.flink.table.api.Types
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.functions.TableAggregateFunction.RetractableCollector
import org.apache.flink.util.Collector

/********************* Top2 with emitUpdateWith(out)Retract ***************/

class Top2EmitUpdateAccum {
  var first: JInt = _
  var second: JInt = _
  var oldFirst: JInt = _
  var oldSecond: JInt = _
}

class Top2EmitUpdate extends TableAggregateFunction[JTuple2[JInt, JInt], Top2EmitUpdateAccum] {

  override def createAccumulator(): Top2EmitUpdateAccum = {
    val acc = new Top2EmitUpdateAccum
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc.oldFirst = Int.MinValue
    acc.oldSecond = Int.MinValue
    acc
  }

  def accumulate(acc: Top2EmitUpdateAccum, v: Int) {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def merge(acc: Top2EmitUpdateAccum, its: JIterable[Top2EmitUpdateAccum]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val top2 = iter.next()
      accumulate(acc, top2.first)
      accumulate(acc, top2.second)
    }
  }

  def emitUpdateWithRetract(
    acc: Top2EmitUpdateAccum,
    out: RetractableCollector[JTuple2[JInt, JInt]])
  : Unit = {
    if (acc.first != acc.oldFirst) {
      // if there is an update, retract old value then emit new value.
      if (acc.oldFirst != Int.MinValue) {
        out.retract(JTuple2.of(acc.oldFirst, 1))
      }
      out.collect(JTuple2.of(acc.first, 1))
      acc.oldFirst = acc.first
    }
    if (acc.second != acc.oldSecond) {
      // if there is an update, retract old value then emit new value.
      if (acc.oldSecond != Int.MinValue) {
        out.retract(JTuple2.of(acc.oldSecond, 2))
      }
      out.collect(JTuple2.of(acc.second, 2))
      acc.oldSecond = acc.second
    }
  }

  def emitUpdateWithoutRetract(
    acc: Top2EmitUpdateAccum,
    out: RetractableCollector[JTuple2[JInt, JInt]])
  : Unit = {
    if (acc.first != acc.oldFirst) {
      // if there is an update, emit new value directly.
      out.collect(JTuple2.of(acc.first, 1))
      acc.oldFirst = acc.first
    }
    if (acc.second != acc.oldSecond) {
      // if there is an update, emit new value directly.
      out.collect(JTuple2.of(acc.second, 2))
      acc.oldSecond = acc.second
    }
  }
}

/********************* Top3 with emitValue ***************/

class Top3Accum {
  var data: util.HashMap[JInt, JInt] = _
  var size: JInt = _
  var smallest: JInt = _
}

class Top3 extends TableAggregateFunction[JTuple2[JInt, JInt], Top3Accum] {
  override def createAccumulator(): Top3Accum = {
    val acc = new Top3Accum
    acc.data = new util.HashMap[JInt, JInt]()
    acc.size = 0
    acc.smallest = Integer.MAX_VALUE
    acc
  }

  def add(acc: Top3Accum, v: Int): Unit = {
    var cnt = acc.data.get(v)
    acc.size += 1
    if (cnt == null) {
      cnt = 0
    }
    acc.data.put(v, cnt + 1)
  }

  def delete(acc: Top3Accum, v: Int): Unit = {
    if (acc.data.containsKey(v)) {
      acc.size -= 1
      val cnt = acc.data.get(v) - 1
      if (cnt == 0) {
        acc.data.remove(v)
      } else {
        acc.data.put(v, cnt)
      }
    }
  }

  def updateSmallest(acc: Top3Accum): Unit = {
    acc.smallest = Integer.MAX_VALUE
    val keys = acc.data.keySet().iterator()
    while (keys.hasNext) {
      val key = keys.next()
      if (key < acc.smallest) {
        acc.smallest = key
      }
    }
  }

  def accumulate(acc: Top3Accum, v: Int) {
    if (acc.size == 0) {
      acc.size = 1
      acc.smallest = v
      acc.data.put(v, 1)
    } else if (acc.size < 3) {
      add(acc, v)
      if (v < acc.smallest) {
        acc.smallest = v
      }
    } else if (v > acc.smallest) {
      delete(acc, acc.smallest)
      add(acc, v)
      updateSmallest(acc)
    }
  }

  def merge(acc: Top3Accum, its: JIterable[Top3Accum]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val map = iter.next().data
      val mapIter = map.entrySet().iterator()
      while (mapIter.hasNext) {
        val entry = mapIter.next()
        for (_ <- 0 until entry.getValue) {
          accumulate(acc, entry.getKey)
        }
      }
    }
  }

  def emitValue(acc: Top3Accum, out: Collector[JTuple2[JInt, JInt]]): Unit = {
    val entries = acc.data.entrySet().iterator()
    while (entries.hasNext) {
      val pair = entries.next()
      for (_ <- 0 until pair.getValue) {
        out.collect(JTuple2.of(pair.getKey, pair.getKey))
      }
    }
  }
}

/********************* Top3 with MapView ***************/

class Top3WithMapViewAccum {
  var data: MapView[JInt, JInt] = _
  var size: JInt = _
  var smallest: JInt = _
}

class Top3WithMapView extends TableAggregateFunction[JTuple2[JInt, JInt], Top3WithMapViewAccum] {

  @Override
  def createAccumulator(): Top3WithMapViewAccum = {
    val acc = new Top3WithMapViewAccum
    acc.data = new MapView(Types.INT, Types.INT)
    acc.size = 0
    acc.smallest = Integer.MAX_VALUE
    acc
  }

  def add(acc: Top3WithMapViewAccum, v: Int): Unit = {
    var cnt = acc.data.get(v)
    acc.size += 1
    if (cnt == null) {
      cnt = 0
    }
    acc.data.put(v, cnt + 1)
  }

  def delete(acc: Top3WithMapViewAccum, v: Int): Unit = {
    if (acc.data.contains(v)) {
      acc.size -= 1
      val cnt = acc.data.get(v) - 1
      if (cnt == 0) {
        acc.data.remove(v)
      } else {
        acc.data.put(v, cnt)
      }
    }
  }

  def updateSmallest(acc: Top3WithMapViewAccum): Unit = {
    acc.smallest = Integer.MAX_VALUE
    val keys = acc.data.iterator
    while (keys.hasNext) {
      val pair = keys.next()
      if (pair.getKey < acc.smallest) {
        acc.smallest = pair.getKey
      }
    }
  }

  def accumulate(acc: Top3WithMapViewAccum, v: Int) {
    if (acc.size == 0) {
      acc.size = 1
      acc.smallest = v
      acc.data.put(v, 1)
    } else if (acc.size < 3) {
      add(acc, v)
      if (v < acc.smallest) {
        acc.smallest = v
      }
    } else if (v > acc.smallest) {
      delete(acc, acc.smallest)
      add(acc, v)
      updateSmallest(acc)
    }
  }

  def emitValue(acc: Top3WithMapViewAccum, out: Collector[JTuple2[JInt, JInt]]): Unit = {
    val keys = acc.data.iterator
    while (keys.hasNext) {
      val pair = keys.next()
      for (_ <- 0 until pair.getValue) {
        out.collect(JTuple2.of(pair.getKey, pair.getKey))
      }
    }
  }
}

/********************* Top3 with retract input ***************/

class Top3WithRetractInputAccum {
  var data: util.LinkedList[JInt] = _
}

/**
  * Top3 with retract input. In this case, we need to store all input data, because every input may
  * be retracted later.
  */
class Top3WithRetractInput
  extends TableAggregateFunction[JTuple2[JInt, JInt], Top3WithRetractInputAccum] {

  override def createAccumulator(): Top3WithRetractInputAccum = {
    val acc = new Top3WithRetractInputAccum
    acc.data = new util.LinkedList[JInt]()
    acc
  }

  def accumulate(acc: Top3WithRetractInputAccum, v: Int) {
    acc.data.addLast(v)
  }

  def retract(acc: Top3WithRetractInputAccum, v: Int) {
    acc.data.removeFirstOccurrence(v)
  }

  def emitValue(acc: Top3WithRetractInputAccum, out: Collector[JTuple2[JInt, JInt]]): Unit = {
    Collections.sort(acc.data, new Comparator[JInt] {
      override def compare(o1: JInt, o2: JInt): Int = {
        o2 - o1
      }
    })
    val ite = acc.data.iterator()
    var i = 0
    while (ite.hasNext && i < 3) {
      i += 1
      val v = ite.next()
      out.collect(JTuple2.of(v, v))
    }
  }
}

/********************* Empty table aggregate functions ***************/

/**
  * Test function for plan test.
  */
class EmptyTableAggFuncWithoutEmit extends TableAggregateFunction[JTuple2[JInt, JInt], Top3Accum] {

  override def createAccumulator(): Top3Accum = new Top3Accum

  def accumulate(acc: Top3Accum, category: Long, value: Timestamp): Unit = {}

  def accumulate(acc: Top3Accum, category: Long, value: Int): Unit = {}

  def accumulate(acc: Top3Accum, value: Int): Unit = {}
}

class EmptyTableAggFunc extends EmptyTableAggFuncWithoutEmit {

  def emitValue(acc: Top3Accum, out: Collector[JTuple2[JInt, JInt]]): Unit = {}
}

class EmptyTableAggFuncWithIntResultType extends TableAggregateFunction[JInt, Top3Accum] {

  override def createAccumulator(): Top3Accum = new Top3Accum

  def accumulate(acc: Top3Accum, value: Int): Unit = {}

  def emitValue(acc: Top3Accum, out: Collector[JInt]): Unit = {}
}

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

import java.lang.{Integer => JInt, Iterable => JIterable}
import java.sql.Timestamp
import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.IntType
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**** Note: Functions in this class suffer performance problem. Only use it in tests. ****/


/****** Function for testing basic functionality of TableAggregateFunction ******/

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

/****** Function for testing MapView ******/

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

/****** Function for testing retract input ******/

class Top3WithRetractInputAcc {
  var data: ListBuffer[Int] = _
}

class Top3WithRetractInput
  extends TableAggregateFunction[JTuple2[JInt, JInt], Top3WithRetractInputAcc] {

  @Override
  def createAccumulator(): Top3WithRetractInputAcc = {
    val acc = new Top3WithRetractInputAcc
    acc.data = new ListBuffer[Int]
    acc
  }

  def accumulate(acc: Top3WithRetractInputAcc, v: Int) {
    acc.data.append(v)
  }

  def retract(acc: Top3WithRetractInputAcc, v: Int) {
    acc.data.remove(acc.data.indexOf(v))
  }

  def emitValue(acc: Top3WithRetractInputAcc, out: Collector[JTuple2[JInt, JInt]]): Unit = {
    acc.data = acc.data.sorted.reverse
    val ite = acc.data.iterator
    var i = 0
    while (i < 3 && i < acc.data.size) {
      val v = ite.next()
      i += 1
      out.collect(JTuple2.of(v, v))
    }
  }
}

/****** Function for testing internal accumulator type ******/

class TableAggSum extends TableAggregateFunction[JInt, GenericRow] {

  override def createAccumulator(): GenericRow = {
    val acc = new GenericRow(1)
    acc.setInt(0, 0)
    acc
  }

  def accumulate(acc: GenericRow, v: Int): Unit = {
    acc.setInt(0, acc.getInt(0) + v)
  }

  def emitValue(acc: GenericRow, out: Collector[JInt]): Unit = {
    // output two records
    val result = acc.getInt(0)
    out.collect(result)
    out.collect(result)
  }

  override def getAccumulatorType: TypeInformation[GenericRow] = {
    new BaseRowTypeInfo(new IntType()).asInstanceOf[TypeInformation[GenericRow]]
  }
}

/**
  * Test function for plan test.
  */
class EmptyTableAggFunc extends TableAggregateFunction[JTuple2[JInt, JInt], Top3Accum] {

  override def createAccumulator(): Top3Accum = new Top3Accum

  def accumulate(acc: Top3Accum, catagory: Timestamp, value: Timestamp): Unit = {}

  def accumulate(acc: Top3Accum, category: Long, value: Timestamp): Unit = {}

  def accumulate(acc: Top3Accum, category: Long, value: Int): Unit = {}

  def accumulate(acc: Top3Accum, value: Int): Unit = {}

  def emitValue(acc: Top3Accum, out: Collector[JTuple2[JInt, JInt]]): Unit = {}
}

class EmptyTableAggFuncWithIntResultType extends TableAggregateFunction[JInt, Top3Accum] {

  override def createAccumulator(): Top3Accum = new Top3Accum

  def accumulate(acc: Top3Accum, value: Int): Unit = {}

  def emitValue(acc: Top3Accum, out: Collector[JInt]): Unit = {}
}

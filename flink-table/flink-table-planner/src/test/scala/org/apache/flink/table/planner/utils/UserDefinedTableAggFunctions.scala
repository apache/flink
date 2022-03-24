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

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.table.functions.python.{PythonEnv, PythonFunction}
import org.apache.flink.table.planner.JLong
import org.apache.flink.util.Collector

import java.lang.{Integer => JInt, Iterable => JIterable}
import java.sql.Timestamp
import java.util

import scala.collection.mutable.ListBuffer

/**** Note: Functions in this class suffer performance problem. Only use it in tests. ****/


/****** Function for testing basic functionality of TableAggregateFunction ******/

class Top3Accum {
  var data: util.Map[JInt, JInt] = _
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

  def add(acc: Top3Accum, v: JInt): Unit = {
    var cnt = acc.data.get(v)
    acc.size += 1
    if (cnt == null) {
      cnt = 0
    }
    acc.data.put(v, cnt + 1)
  }

  def delete(acc: Top3Accum, v: JInt): Unit = {
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

  def accumulate(acc: Top3Accum, v: JInt) {
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
    acc.data = new MapView()
    acc.size = 0
    acc.smallest = Integer.MAX_VALUE
    acc
  }

  def add(acc: Top3WithMapViewAccum, v: JInt): Unit = {
    var cnt = acc.data.get(v)
    acc.size += 1
    if (cnt == null) {
      cnt = 0
    }
    acc.data.put(v, cnt + 1)
  }

  def delete(acc: Top3WithMapViewAccum, v: JInt): Unit = {
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

  def accumulate(acc: Top3WithMapViewAccum, v: JInt) {
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
  @DataTypeHint("RAW")
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

  def accumulate(acc: Top3WithRetractInputAcc, v: JInt) {
    acc.data.append(v)
  }

  def retract(acc: Top3WithRetractInputAcc, v: JInt) {
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

@FunctionHint(accumulator = new DataTypeHint(value = "ROW<i INT>", bridgedTo = classOf[RowData]))
class TableAggSum extends TableAggregateFunction[JInt, RowData] {

  override def createAccumulator(): RowData = {
    val acc = new GenericRowData(1)
    acc.setField(0, 0)
    acc
  }

  def accumulate(rowData: RowData, v: JInt): Unit = {
    val acc = rowData.asInstanceOf[GenericRowData]
    acc.setField(0, acc.getInt(0) + v)
  }

  def emitValue(rowData: RowData, out: Collector[JInt]): Unit = {
    val acc = rowData.asInstanceOf[GenericRowData]
    // output two records
    val result = acc.getInt(0)
    out.collect(result)
    out.collect(result)
  }
}

/**
  * Test function for plan test.
  */
class EmptyTableAggFunc extends TableAggregateFunction[JTuple2[JInt, JInt], Top3Accum] {

  override def createAccumulator(): Top3Accum = new Top3Accum

  def accumulate(acc: Top3Accum, category: Timestamp, value: Timestamp): Unit = {}

  def accumulate(acc: Top3Accum, category: JLong, value: Timestamp): Unit = {}

  def accumulate(acc: Top3Accum, category: JLong, value: JInt): Unit = {}

  def accumulate(acc: Top3Accum, value: JInt): Unit = {}

  def emitValue(acc: Top3Accum, out: Collector[JTuple2[JInt, JInt]]): Unit = {}
}

class EmptyTableAggFuncWithIntResultType extends TableAggregateFunction[JInt, Top3Accum] {

  override def createAccumulator(): Top3Accum = new Top3Accum

  def accumulate(acc: Top3Accum, value: JInt): Unit = {}

  def emitValue(acc: Top3Accum, out: Collector[JInt]): Unit = {}
}

class PythonEmptyTableAggFunc
  extends TableAggregateFunction[JTuple2[JInt, JInt], Top3Accum]
  with PythonFunction {

  override def getSerializedPythonFunction: Array[Byte] = Array(0)

  override def getPythonEnv: PythonEnv = null

  override def createAccumulator(): Top3Accum = new Top3Accum

  def accumulate(acc: Top3Accum, value1: JInt, value2: JInt): Unit = {}

  def emitValue(acc: Top3Accum, out: Collector[JTuple2[JInt, JInt]]): Unit = {}
}

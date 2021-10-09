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

package org.apache.flink.table.legacyutils

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.sinks.{AppendStreamTableSink, RetractStreamTableSink, TableSink, UpsertStreamTableSink}
import org.apache.flink.types.Row

import java.lang.{Boolean => JBool}

import scala.collection.mutable

/*
 * Testing utils adopted from legacy planner until the Python code is updated.
 */

@deprecated
private[flink] class TestAppendSink extends AppendStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _

  override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
    dataStream.map(
      new MapFunction[Row, JTuple2[JBool, Row]] {
        override def map(value: Row): JTuple2[JBool, Row] = new JTuple2(true, value)
      })
      .addSink(new RowSink)
  }

  override def getOutputType: TypeInformation[Row] = new RowTypeInfo(fTypes, fNames)

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def configure(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = {
    val copy = new TestAppendSink
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy
  }
}

@deprecated
private[flink] class TestRetractSink extends RetractStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _

  override def consumeDataStream(s: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    s.addSink(new RowSink)
  }

  override def getRecordType: TypeInformation[Row] = new RowTypeInfo(fTypes, fNames)

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[JTuple2[JBool, Row]] = {
    val copy = new TestRetractSink
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy
  }
}

@deprecated
private[flink] class TestUpsertSink(
    expectedKeys: Array[String],
    expectedIsAppendOnly: Boolean)
  extends UpsertStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _

  override def setKeyFields(keys: Array[String]): Unit =
    if (keys != null) {
      if (!expectedKeys.sorted.mkString(",").equals(keys.sorted.mkString(","))) {
        throw new AssertionError("Provided key fields do not match expected keys")
      }
    } else {
      if (expectedKeys != null) {
        throw new AssertionError("Provided key fields should not be null.")
      }
    }

  override def setIsAppendOnly(isAppendOnly: JBool): Unit =
    if (expectedIsAppendOnly != isAppendOnly) {
      throw new AssertionError("Provided isAppendOnly does not match expected isAppendOnly")
    }

  override def getRecordType: TypeInformation[Row] = new RowTypeInfo(fTypes, fNames)

  override def consumeDataStream(s: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    s.addSink(new RowSink)
  }

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[JTuple2[JBool, Row]] = {
    val copy = new TestUpsertSink(expectedKeys, expectedIsAppendOnly)
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy
  }
}

@deprecated
class RowSink extends SinkFunction[JTuple2[JBool, Row]] {
  override def invoke(value: JTuple2[JBool, Row]): Unit = RowCollector.addValue(value)
}

@deprecated
object RowCollector {
  private val sink: mutable.ArrayBuffer[JTuple2[JBool, Row]] =
    new mutable.ArrayBuffer[JTuple2[JBool, Row]]()

  def addValue(value: JTuple2[JBool, Row]): Unit = {

    // make a copy
    val copy = new JTuple2[JBool, Row](value.f0, Row.copy(value.f1))
    sink.synchronized {
      sink += copy
    }
  }

  def getAndClearValues: List[JTuple2[JBool, Row]] = {
    val out = sink.toList
    sink.clear()
    out
  }

  /** Converts a list of retraction messages into a list of final results. */
  def retractResults(results: List[JTuple2[JBool, Row]]): List[String] = {

    val retracted = results
      .foldLeft(Map[String, Int]()){ (m: Map[String, Int], v: JTuple2[JBool, Row]) =>
        val cnt = m.getOrElse(v.f1.toString, 0)
        if (v.f0) {
          m + (v.f1.toString -> (cnt + 1))
        } else {
          m + (v.f1.toString -> (cnt - 1))
        }
      }.filter{ case (_, c: Int) => c != 0 }

    if (retracted.exists{ case (_, c: Int) => c < 0}) {
      throw new AssertionError("Received retracted rows which have not been accumulated.")
    }

    retracted.flatMap { case (r: String, c: Int) => (0 until c).map(_ => r) }.toList
  }

  /** Converts a list of upsert messages into a list of final results. */
  def upsertResults(results: List[JTuple2[JBool, Row]], keys: Array[Int]): List[String] = {

    def getKeys(r: Row): Row = Row.project(r, keys)

    val upserted = results.foldLeft(Map[Row, String]()){ (o: Map[Row, String], r) =>
      val key = getKeys(r.f1)
      if (r.f0) {
        o + (key -> r.f1.toString)
      } else {
        o - key
      }
    }

    upserted.values.toList
  }
}

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

package org.apache.flink.table.sinks

import java.lang.{Boolean => JBool}
import java.util.TimeZone
import java.util.{Date => JDate}
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.runtime.functions.DateTimeFunctions
import org.apache.flink.util.StringUtils

/**
  * A simple [[TableSink]] to output data to console.
  *
  */
class PrintTableSink(tz: TimeZone)
  extends TableSinkBase[JTuple2[JBool, Row]]
  with BatchCompatibleStreamTableSink[JTuple2[JBool, Row]]
  with UpsertStreamTableSink[Row] {

  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]) = {
    val sink: PrintSinkFunction = new PrintSinkFunction(tz)
    dataStream.addSink(sink).name(sink.toString)
  }

  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = new PrintTableSink(tz)

  override def setKeyFields(keys: Array[String]): Unit = {}

  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {}

  override def getRecordType: DataType = DataTypes.createRowType(getFieldTypes, getFieldNames)

  /** Emits the DataStream. */
  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]]) = {
    val sink: PrintSinkFunction = new PrintSinkFunction(tz)
    boundedStream.addSink(sink).name(sink.toString)
  }
}

/**
  * Implementation of the SinkFunction writing every tuple to the standard output.
  *
  */
class PrintSinkFunction(tz: TimeZone) extends RichSinkFunction[JTuple2[JBool, Row]] {
  private var prefix: String = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val context = getRuntimeContext.asInstanceOf[StreamingRuntimeContext]
    prefix = "task-" + (context.getIndexOfThisSubtask + 1) + "> "
  }

  override def invoke(in: JTuple2[JBool, Row]): Unit = {
    val sb = new StringBuilder
    val row = in.f1
    for (i <- 0 to row.getArity - 1) {
      if (i > 0) sb.append(",")
      val f = row.getField(i)
      if (f.isInstanceOf[Date]) {
        sb.append(DateTimeFunctions.dateFormat(f.asInstanceOf[JDate].getTime, "yyyy-MM-dd", tz))
      } else if (f.isInstanceOf[Time]) {
        sb.append(DateTimeFunctions.dateFormat(f.asInstanceOf[JDate].getTime, "HH:mm:ss", tz))
      } else if (f.isInstanceOf[Timestamp]) {
        sb.append(DateTimeFunctions.dateFormat(f.asInstanceOf[JDate].getTime,
          "yyyy-MM-dd HH:mm:ss.SSS", tz))
      } else {
        sb.append(StringUtils.arrayAwareToString(f))
      }
    }

    if (in.f0) {
      System.out.println(prefix + "(+)" + sb.toString())
    } else {
      System.out.println(prefix + "(-)" + sb.toString())
    }
  }

  override def close(): Unit = {
    this.prefix = ""
  }

  override def toString: String = "Print to System.out"
}

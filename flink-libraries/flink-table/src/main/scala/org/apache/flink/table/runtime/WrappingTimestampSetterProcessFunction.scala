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

package org.apache.flink.table.runtime

import java.sql.Timestamp

import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.util.Collector

/**
  * Wraps a ProcessFunction and sets a Timestamp field of a CRow as
  * [[org.apache.flink.streaming.runtime.streamrecord.StreamRecord]] timestamp.
  */
class WrappingTimestampSetterProcessFunction[OUT](
    function: MapFunction[CRow, OUT],
    rowtimeIdx: Int)
  extends ProcessFunction[CRow, OUT] {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    function match {
      case f: RichMapFunction[_, _] =>
        f.setRuntimeContext(getRuntimeContext)
        f.open(parameters)
      case _ =>
    }
  }

  override def processElement(
      in: CRow,
      ctx: ProcessFunction[CRow, OUT]#Context,
      out: Collector[OUT]): Unit = {

    val timestamp = SqlFunctions.toLong(in.row.getField(rowtimeIdx).asInstanceOf[Timestamp])
    out.asInstanceOf[TimestampedCollector[_]].setAbsoluteTimestamp(timestamp)

    out.collect(function.map(in))
  }

}

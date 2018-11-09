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

import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.util.Collector

/**
  * Wraps a ProcessFunction and sets a Timestamp field of a CRow as
  * [[org.apache.flink.streaming.runtime.streamrecord.StreamRecord]] timestamp.
  */
class OutputRowtimeProcessFunction[OUT](
    function: MapFunction[CRow, OUT],
    rowtimeIdx: Int)
  extends ProcessFunction[CRow, OUT] {

  override def open(parameters: Configuration): Unit = {
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def processElement(
      in: CRow,
      ctx: ProcessFunction[CRow, OUT]#Context,
      out: Collector[OUT]): Unit = {

    val timestamp = in.row.getField(rowtimeIdx).asInstanceOf[Long]
    out.asInstanceOf[TimestampedCollector[_]].setAbsoluteTimestamp(timestamp)

    val convertedTimestamp = SqlFunctions.internalToTimestamp(timestamp)
    in.row.setField(rowtimeIdx, convertedTimestamp)

    out.collect(function.map(in))
  }

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }
}

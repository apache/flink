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
package org.apache.flink.table.runtime.aggregate

import java.sql.Timestamp

import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

/**
  * This map function only works for windows on batch tables.
  * It appends an (aligned) rowtime field to the end of the output row.
  *
  * @param genAggregations      Code-generated [[GeneratedAggregations]]
  * @param timeFieldPos         Time field position in input row
  * @param tumbleTimeWindowSize The size of tumble time window
  */
class DataSetWindowAggMapFunction(
    private val genAggregations: GeneratedAggregationsFunction,
    private val timeFieldPos: Int,
    private val tumbleTimeWindowSize: Option[Long],
    @transient private val returnType: TypeInformation[Row])
  extends RichMapFunction[Row, Row]
    with ResultTypeQueryable[Row]
    with Compiler[GeneratedAggregations]
    with Logging {

  private var accs: Row = _
  private var output: Row = _

  private var function: GeneratedAggregations = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    accs = function.createAccumulators()
    output = function.createOutputRow()
  }

  override def map(input: Row): Row = {

    function.resetAccumulator(accs)

    function.accumulate(accs, input)

    function.setAggregationResults(accs, output)

    function.setForwardedFields(input, output)

    val timeField = input.getField(timeFieldPos)
    val rowtime = getTimestamp(timeField)
    val rowtimeIndex = output.getArity - 1
    if (tumbleTimeWindowSize.isDefined) {
      // in case of tumble time window, align rowtime to window start to represent the window
      output.setField(
        rowtimeIndex,
        TimeWindow.getWindowStartWithOffset(rowtime, 0L, tumbleTimeWindowSize.get))
    } else {
      // for session window and slide window
      output.setField(rowtimeIndex, rowtime)
    }

    output
  }

  private def getTimestamp(timeField: Any): Long = {
    timeField match {
      case b: Byte => b.toLong
      case t: Character => t.toLong
      case s: Short => s.toLong
      case i: Int => i.toLong
      case l: Long => l
      case f: Float => f.toLong
      case d: Double => d.toLong
      case s: String => s.toLong
      case t: Timestamp => SqlFunctions.toLong(t)
      case _ =>
        throw new RuntimeException(
          s"Window time field doesn't support ${timeField.getClass} type currently")
    }
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}


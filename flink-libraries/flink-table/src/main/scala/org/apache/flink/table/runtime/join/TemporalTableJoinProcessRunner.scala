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
package org.apache.flink.table.runtime.join

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.runtime.collector.TableFunctionCollector
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

class TemporalTableJoinProcessRunner(
    fetcherName: String,
    var fetcherCode: String,
    collectorName: String,
    var collectorCode: String,
    leftOuterJoin: Boolean,
    inputFieldTypes: Array[InternalType],
    @transient returnType: BaseRowTypeInfo)
  extends ProcessFunction[BaseRow, BaseRow]
  with ResultTypeQueryable[BaseRow]
  with Compiler[Any]
  with Logging {

  var fetcher: FlatMapFunction[BaseRow, BaseRow] = _
  var collector: TableFunctionCollector[BaseRow] = _
  val rightArity: Int = returnType.getArity - inputFieldTypes.length

  @transient var nullRow: BaseRow = _
  @transient var outRow: JoinedRow = _

  override def open(parameters: Configuration): Unit = {
    val collectorClasss = compile(
      getRuntimeContext.getUserCodeClassLoader,
      collectorName,
      collectorCode)
    collectorCode = null
    collector = collectorClasss.newInstance().asInstanceOf[TableFunctionCollector[BaseRow]]

    val fetcherClass = compile(
      getRuntimeContext.getUserCodeClassLoader,
      fetcherName,
      fetcherCode)
    fetcherCode = null
    fetcher = fetcherClass.newInstance().asInstanceOf[FlatMapFunction[BaseRow, BaseRow]]

    FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext)
    FunctionUtils.openFunction(fetcher, parameters)

    nullRow = new GenericRow(rightArity)
    outRow = new JoinedRow()
  }

  override def processElement(
    in: BaseRow,
    ctx: ProcessFunction[BaseRow, BaseRow]#Context,
    out: Collector[BaseRow]): Unit = {

    collector.setCollector(out)
    collector.setInput(in)
    collector.reset()

    // fetcher has copied the input field when object reuse is enabled
    fetcher.flatMap(in, getFetcherCollector)

    if (leftOuterJoin && !collector.isCollected) {
      outRow.replace(in, nullRow)
      outRow.setHeader(in.getHeader)
      out.collect(outRow)
    }
  }

  def getFetcherCollector: Collector[BaseRow] = collector

  override def getProducedType: BaseRowTypeInfo =
    returnType.asInstanceOf[BaseRowTypeInfo]

  override def close(): Unit = {
    FunctionUtils.closeFunction(fetcher)
  }
}

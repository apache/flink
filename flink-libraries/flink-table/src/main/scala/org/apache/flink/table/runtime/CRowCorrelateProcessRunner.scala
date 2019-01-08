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

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * A CorrelateProcessRunner with [[CRow]] input and [[CRow]] output.
  */
class CRowCorrelateProcessRunner(
    processName: String,
    processCode: String,
    collectorName: String,
    collectorCode: String,
    @transient var returnType: TypeInformation[CRow])
  extends ProcessFunction[CRow, CRow]
  with ResultTypeQueryable[CRow]
  with Compiler[Any]
  with Logging {

  private var function: ProcessFunction[Row, Row] = _
  private var collector: TableFunctionCollector[_] = _
  private var cRowWrapper: CRowWrappingCollector = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling TableFunctionCollector: $collectorName \n\n Code:\n$collectorCode")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, collectorName, collectorCode)
    LOG.debug("Instantiating TableFunctionCollector.")
    collector = clazz.newInstance().asInstanceOf[TableFunctionCollector[_]]
    this.cRowWrapper = new CRowWrappingCollector()

    LOG.debug(s"Compiling ProcessFunction: $processName \n\n Code:\n$processCode")
    val processClazz = compile(getRuntimeContext.getUserCodeClassLoader, processName, processCode)
    val constructor = processClazz.getConstructor(classOf[TableFunctionCollector[_]])
    LOG.debug("Instantiating ProcessFunction.")
    function = constructor.newInstance(collector).asInstanceOf[ProcessFunction[Row, Row]]
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def processElement(
      in: CRow,
      ctx: ProcessFunction[CRow, CRow]#Context,
      out: Collector[CRow])
    : Unit = {

    cRowWrapper.out = out
    cRowWrapper.setChange(in.change)

    collector.setCollector(cRowWrapper)
    collector.setInput(in.row)
    collector.reset()

    function.processElement(
      in.row,
      ctx.asInstanceOf[ProcessFunction[Row, Row]#Context],
      cRowWrapper)
  }

  override def getProducedType: TypeInformation[CRow] = returnType

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }
}

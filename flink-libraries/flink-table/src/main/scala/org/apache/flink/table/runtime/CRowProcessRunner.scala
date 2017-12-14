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
  * ProcessRunner with [[CRow]] input and [[CRow]] output.
  */
class CRowProcessRunner(
    name: String,
    code: String,
    @transient var returnType: TypeInformation[CRow])
  extends ProcessFunction[CRow, CRow]
  with ResultTypeQueryable[CRow]
  with Compiler[ProcessFunction[Row, Row]]
  with Logging {

  private var function: ProcessFunction[Row, Row] = _
  private var cRowWrapper: CRowWrappingCollector = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling ProcessFunction: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating ProcessFunction.")
    function = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)

    this.cRowWrapper = new CRowWrappingCollector()
  }

  override def processElement(
      in: CRow,
      ctx: ProcessFunction[CRow, CRow]#Context,
      out: Collector[CRow])
    : Unit = {

    cRowWrapper.out = out
    cRowWrapper.setChange(in.change)
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



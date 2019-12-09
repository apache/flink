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

package org.apache.flink.table.runtime.`match`

import java.util

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * PatternSelectFunctionRunner with [[Row]] input and [[CRow]] output.
  */
class PatternProcessFunctionRunner(
    name: String,
    code: String)
  extends PatternProcessFunction[Row, CRow]
  with Compiler[PatternProcessFunction[Row, Row]]
  with Logging {

  @transient private var cRowWrappingCollector : CRowWrappingCollector = _

  @transient private var function: PatternProcessFunction[Row, Row] = _

  override def open(parameters: Configuration): Unit = {
    this.cRowWrappingCollector = new CRowWrappingCollector

    LOG.debug(s"Compiling PatternProcessFunction: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating PatternProcessFunction.")
    function = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def processMatch(
      `match`: util.Map[String, util.List[Row]],
      ctx: PatternProcessFunction.Context,
      out: Collector[CRow]): Unit = {

    out.asInstanceOf[TimestampedCollector[_]].eraseTimestamp()
    cRowWrappingCollector.out = out
    function.processMatch(`match`, ctx, cRowWrappingCollector)
  }

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }
}


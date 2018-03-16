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
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

class CorrelateFlatMapRunner[IN, OUT](
    flatMapName: String,
    flatMapCode: String,
    collectorName: String,
    collectorCode: String,
    @transient var returnType: TypeInformation[OUT])
  extends RichFlatMapFunction[IN, OUT]
  with ResultTypeQueryable[OUT]
  with Compiler[Any]
  with Logging {

  private var function: FlatMapFunction[IN, OUT] = _
  private var collector: TableFunctionCollector[_] = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling TableFunctionCollector: $collectorName \n\n Code:\n$collectorCode")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, collectorName, collectorCode)
    LOG.debug("Instantiating TableFunctionCollector.")
    collector = clazz.newInstance().asInstanceOf[TableFunctionCollector[_]]

    LOG.debug(s"Compiling FlatMapFunction: $flatMapName \n\n Code:\n$flatMapCode")
    val flatMapClazz = compile(getRuntimeContext.getUserCodeClassLoader, flatMapName, flatMapCode)
    val constructor = flatMapClazz.getConstructor(classOf[TableFunctionCollector[_]])
    LOG.debug("Instantiating FlatMapFunction.")
    function = constructor.newInstance(collector).asInstanceOf[FlatMapFunction[IN, OUT]]
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def flatMap(in: IN, out: Collector[OUT]): Unit = {
    collector.setCollector(out)
    collector.setInput(in)
    collector.reset()
    function.flatMap(in, out)
  }

  override def getProducedType: TypeInformation[OUT] = returnType

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }
}

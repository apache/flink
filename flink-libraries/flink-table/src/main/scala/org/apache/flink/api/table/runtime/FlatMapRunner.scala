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

package org.apache.flink.api.table.runtime

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class FlatMapRunner[IN, OUT](
    name: String,
    code: String,
    @transient returnType: TypeInformation[OUT])
  extends RichFlatMapFunction[IN, OUT]
  with ResultTypeQueryable[OUT]
  with FunctionCompiler[FlatMapFunction[IN, OUT]] {

  val LOG = LoggerFactory.getLogger(this.getClass)

  private var function: FlatMapFunction[IN, OUT] = null

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling FlatMapFunction: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating FlatMapFunction.")
    function = clazz.newInstance()
  }

  override def flatMap(in: IN, out: Collector[OUT]): Unit =
    function.flatMap(in, out)

  override def getProducedType: TypeInformation[OUT] = returnType
}

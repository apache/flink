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

import org.apache.flink.api.common.functions.{FlatJoinFunction, RichFlatJoinFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

class FlatJoinRunner[IN1, IN2, OUT](
    name: String,
    code: String,
    @transient var returnType: TypeInformation[OUT])
  extends RichFlatJoinFunction[IN1, IN2, OUT]
  with ResultTypeQueryable[OUT]
  with Compiler[FlatJoinFunction[IN1, IN2, OUT]]
  with Logging {

  private var function: FlatJoinFunction[IN1, IN2, OUT] = null

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling FlatJoinFunction: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating FlatJoinFunction.")
    function = clazz.newInstance()
  }

  override def join(first: IN1, second: IN2, out: Collector[OUT]): Unit =
    function.join(first, second, out)

  override def getProducedType: TypeInformation[OUT] = returnType
}

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

package org.apache.flink.table.runtime.io

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging

class TableFunctionSource(
  name: String,
  code: String,
  resultType: TypeInformation[Any])
  extends RichSourceFunction[Any]
  with Compiler[RichSourceFunction[Any]]
  with Logging {

  private var sourceFunction: RichSourceFunction[Any] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    LOG.debug(s"Compiling TableFunctionSource: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating TableFunctionSource.")
    sourceFunction = clazz.newInstance()
    sourceFunction.setRuntimeContext(getRuntimeContext)
    sourceFunction.open(parameters)
  }

  override def close(): Unit = {
    sourceFunction.close()
    super.close()
  }

  override def run(ctx: SourceFunction.SourceContext[Any]): Unit = {
    sourceFunction.run(ctx)
  }

  override def cancel(): Unit = {
    sourceFunction.cancel()
  }
}

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
import org.apache.flink.cep.RichPatternTimeoutFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.{GeneratedClass, GeneratedPatternTimeoutFunction}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.util.Logging

/**
  * PatternTimeoutFunctionRunner with [[BaseRow]] input and [[BaseRow]] output.
  */
class PatternTimeoutFunctionRunner(
    genFunction: GeneratedClass[_])
  extends RichPatternTimeoutFunction[BaseRow, BaseRow]
  with Logging {

  @transient private var function: RichPatternTimeoutFunction[BaseRow, BaseRow] = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling PatternTimeoutFunction: ${genFunction.name} \n\n " +
                s"Code:\n${genFunction.code}")
    function = genFunction.asInstanceOf[GeneratedPatternTimeoutFunction]
      .newInstance(Thread.currentThread().getContextClassLoader)
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def timeout(
      pattern: util.Map[String, util.List[BaseRow]],
      timeoutTimestamp: Long): BaseRow = {
    function.timeout(pattern, timeoutTimestamp)
  }
}

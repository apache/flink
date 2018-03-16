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

import org.apache.flink.api.common.io.{GenericInputFormat, NonParallelInput}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.core.io.GenericInputSplit
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

class ValuesInputFormat(
    name: String,
    code: String,
    @transient var returnType: TypeInformation[Row])
  extends GenericInputFormat[Row]
  with NonParallelInput
  with ResultTypeQueryable[Row]
  with Compiler[GenericInputFormat[Row]]
  with Logging {

  private var format: GenericInputFormat[Row] = _

  override def open(split: GenericInputSplit): Unit = {
    LOG.debug(s"Compiling GenericInputFormat: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating GenericInputFormat.")
    format = clazz.newInstance()
  }

  override def reachedEnd(): Boolean = format.reachedEnd()

  override def nextRecord(reuse: Row): Row = format.nextRecord(reuse)

  override def getProducedType: TypeInformation[Row] = returnType
}

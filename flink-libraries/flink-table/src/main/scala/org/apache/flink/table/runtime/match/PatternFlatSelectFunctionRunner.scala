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

import org.apache.flink.cep.PatternFlatSelectFunction
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * PatternFlatSelectFunctionRunner with [[Row]] input and [[CRow]] output.
  */
class PatternFlatSelectFunctionRunner(
    name: String,
    code: String)
  extends PatternFlatSelectFunction[Row, CRow]
  with Compiler[PatternFlatSelectFunction[Row, Row]] {

  val LOG = LoggerFactory.getLogger(this.getClass)

  private var cRowWrapper: CRowWrappingCollector = _

  private var function: PatternFlatSelectFunction[Row, Row] = _

  def init(): Unit = {
    LOG.debug(s"Compiling PatternFlatSelectFunction: $name \n\n Code:\n$code")
    val clazz = compile(Thread.currentThread().getContextClassLoader, name, code)
    LOG.debug("Instantiating PatternFlatSelectFunction.")
    function = clazz.newInstance()

    this.cRowWrapper = new CRowWrappingCollector()
  }

  override def flatSelect(
    pattern: util.Map[String, util.List[Row]],
    out: Collector[CRow]): Unit = {
    if (function == null) {
      init()
    }

    cRowWrapper.out = out
    function.flatSelect(pattern, cRowWrapper)
  }
}

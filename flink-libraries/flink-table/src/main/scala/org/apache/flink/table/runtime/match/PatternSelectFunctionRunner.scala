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

import java.io.{IOException, ObjectInputStream}
import java.util

import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction}
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * PatternSelectFunctionRunner with [[Row]] input and [[CRow]] output.
  */
class PatternSelectFunctionRunner(
    name: String,
    code: String)
  extends PatternFlatSelectFunction[Row, CRow]
  with Compiler[PatternSelectFunction[Row, Row]]
  with Logging {

  @transient private var outCRow: CRow = _

  @transient private var function: PatternSelectFunction[Row, Row] = _

  def init(): Unit = {
    LOG.debug(s"Compiling PatternSelectFunction: $name \n\n Code:\n$code")
    val clazz = compile(Thread.currentThread().getContextClassLoader, name, code)
    LOG.debug("Instantiating PatternSelectFunction.")
    function = clazz.newInstance()
    // TODO add logic for opening and closing the function once it can be a RichFunction
  }

  override def flatSelect(
      pattern: util.Map[String, util.List[Row]],
      out: Collector[CRow])
    : Unit = {
    outCRow.row = function.select(pattern)
    out.asInstanceOf[TimestampedCollector[_]].eraseTimestamp()
    out.collect(outCRow)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()

    if (outCRow == null) {
      outCRow = new CRow(null, true)
    }

    if (function == null) {
      init()
    }
  }
}


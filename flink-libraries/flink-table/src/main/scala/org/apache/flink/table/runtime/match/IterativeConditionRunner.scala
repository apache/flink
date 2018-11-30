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

import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

/**
  * IterativeConditionRunner with [[Row]] value.
  */
class IterativeConditionRunner(
    name: String,
    code: String)
  extends IterativeCondition[Row]
  with Compiler[IterativeCondition[Row]]
  with Logging {

  @transient private var function: IterativeCondition[Row] = _

  def init(): Unit = {
    LOG.debug(s"Compiling IterativeCondition: $name \n\n Code:\n$code")
    // We cannot get user's classloader currently, see FLINK-6938 for details
    val clazz = compile(Thread.currentThread().getContextClassLoader, name, code)
    LOG.debug("Instantiating IterativeCondition.")
    function = clazz.newInstance()
    // TODO add logic for opening and closing the function once it can be a RichFunction
  }

  override def filter(value: Row, ctx: IterativeCondition.Context[Row]): Boolean = {
    function.filter(value, ctx)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    if (function == null) {
      init()
    }
  }
}

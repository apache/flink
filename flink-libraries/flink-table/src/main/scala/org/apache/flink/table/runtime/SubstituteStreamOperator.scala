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

import org.apache.flink.streaming.api.operators.{ChainingStrategy, StreamOperator}
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging

import scala.collection.mutable

abstract class SubstituteStreamOperator[OUT <: Any](
    name: String,
    var code: String,
    var chainingStrategy: ChainingStrategy = ChainingStrategy.ALWAYS,
    val references: mutable.ArrayBuffer[AnyRef] = new mutable.ArrayBuffer[AnyRef]())
  extends StreamOperator[OUT]
  with Compiler[StreamOperator[OUT]]
  with Logging {

  private var relID: Int = _

  def getActualStreamOperator(cl: ClassLoader): StreamOperator[OUT] = {
    LOG.debug(s"Compiling StreamOperator: $name \n\n Code:\n$code.")
    val clazz = compile(cl, name, code)
    code = null
    LOG.debug("Instantiating StreamOperator.")
    val streamOperator = clazz match {
      case cls if classOf[WithReferences].isAssignableFrom(cls) =>
          clazz.getConstructor(classOf[Array[AnyRef]]).newInstance(references.toArray)
      case _ => clazz.newInstance()
    }
    streamOperator.setChainingStrategy(chainingStrategy)
    streamOperator
  }

  override def getChainingStrategy: ChainingStrategy = {
    chainingStrategy
  }

  override def setChainingStrategy(chainingStrategy: ChainingStrategy): Unit = {
    this.chainingStrategy = chainingStrategy
  }

  def getRelID: Int = relID
}


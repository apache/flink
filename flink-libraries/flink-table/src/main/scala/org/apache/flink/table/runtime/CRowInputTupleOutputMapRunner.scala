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

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}


/**
  * Convert [[CRow]] to a [[Tuple2]]
  */
class CRowInputTupleOutputMapRunner[OUT](
    name: String,
    code: String,
    @transient returnType: TypeInformation[JTuple2[Boolean, OUT]])
  extends RichMapFunction[CRow, JTuple2[Boolean, OUT]]
          with ResultTypeQueryable[JTuple2[Boolean, OUT]]
          with Compiler[MapFunction[Row, OUT]] {

  val LOG = LoggerFactory.getLogger(this.getClass)

  private var function: MapFunction[Row, OUT] = _
  private var tupleWrapper: JTuple2[Boolean, OUT] = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling MapFunction: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating MapFunction.")
    function = clazz.newInstance()
    tupleWrapper = new JTuple2[Boolean, OUT]()
  }

  override def map(in: CRow): JTuple2[Boolean, OUT] = {
    tupleWrapper.f0 = in.change
    tupleWrapper.f1 = function.map(in.row)
    tupleWrapper
  }

  override def getProducedType: TypeInformation[JTuple2[Boolean, OUT]] = returnType
}

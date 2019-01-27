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

import org.apache.flink.streaming.api.operators.{AbstractOneInputSubstituteStreamOperator, ChainingStrategy}

import scala.collection.mutable

class OneInputSubstituteStreamOperator[IN <: Any, OUT <: Any](
    name: String,
    @transient code: String,
    override val references: mutable.ArrayBuffer[AnyRef] = new mutable.ArrayBuffer[AnyRef]())
  extends SubstituteStreamOperator[OUT](name, code, ChainingStrategy.ALWAYS, references)
  with AbstractOneInputSubstituteStreamOperator[IN, OUT] {

  override def endInput(): Unit = {}

  override def requireState(): Boolean = false
}


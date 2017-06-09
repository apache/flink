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

package org.apache.flink.streaming.api.scala.function

import org.apache.flink.annotation.Public
import org.apache.flink.api.common.functions.RichFunction
import org.apache.flink.api.common.state.{ValueStateDescriptor, ValueState}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration

/**
 * Trait implementing the functionality necessary to apply stateful functions in 
 * RichFunctions without exposing the OperatorStates to the user. The user should
 * call the applyWithState method in his own RichFunction implementation.
 */
@Public
trait StatefulFunction[I, O, S] extends RichFunction {

  protected val stateSerializer: TypeSerializer[S]
  
  private[this] var state: ValueState[S] = _
  

  def applyWithState(in: I, fun: (I, Option[S]) => (O, Option[S])): O = {
    val (o, s: Option[S]) = fun(in, Option(state.value()))
    s match {
      case Some(v) => state.update(v)
      case None => state.update(null.asInstanceOf[S])
    }
    o
  }

  override def open(c: Configuration) = {
    val info = new ValueStateDescriptor[S]("state", stateSerializer)
    state = getRuntimeContext().getState(info)
  }
}

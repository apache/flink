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

import java.beans.Transient

import org.apache.flink.annotation.Public
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RichFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.Window

/**
  * Base abstract class for functions that are evaluated over
  * keyed (grouped) windows using a context for retrieving extra information.
  *
  * @tparam IN The type of the input value.
  * @tparam OUT The type of the output value.
  * @tparam W The type of the window.
  */
@Public
abstract class RichProcessAllWindowFunction[IN, OUT, W <: Window]
    extends ProcessAllWindowFunction[IN, OUT, W]
    with RichFunction {

  @Transient
  private var runtimeContext: RuntimeContext = null

  // --------------------------------------------------------------------------------------------
  //  Runtime context access
  // --------------------------------------------------------------------------------------------

  override def setRuntimeContext(t: RuntimeContext) {
    this.runtimeContext = t
  }

  override def getRuntimeContext: RuntimeContext = {
    if (this.runtimeContext != null) {
      this.runtimeContext
    }
    else {
      throw new IllegalStateException("The runtime context has not been initialized.")
    }
  }

  override def getIterationRuntimeContext: IterationRuntimeContext = {
    if (this.runtimeContext == null) {
      throw new IllegalStateException("The runtime context has not been initialized.")
    }
    else {
      this.runtimeContext match {
        case iterationRuntimeContext: IterationRuntimeContext => iterationRuntimeContext
        case _ =>
          throw new IllegalStateException("This stub is not part of an iteration step function.")
      }
    }
  }

  // --------------------------------------------------------------------------------------------
  //  Default life cycle methods
  // --------------------------------------------------------------------------------------------

  @throws[Exception]
  override def open(parameters: Configuration) {
  }

  @throws[Exception]
  override def close() {
  }
}


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

package org.apache.flink.streaming.api.scala.async

import org.apache.flink.api.common.functions.{AbstractRichFunction, IterationRuntimeContext, RuntimeContext}
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction.{RichAsyncFunctionIterationRuntimeContext, RichAsyncFunctionRuntimeContext}
import org.apache.flink.util.Preconditions

/**
  * Rich variant of the [[AsyncFunction]].
  * As a [[org.apache.flink.api.common.functions.RichFunction]], it gives access to
  * the [[RuntimeContext]] and provides setup and teardown methods.
  *
  * @tparam IN The type of the input element
  * @tparam OUT The type of the output elements
  */
trait RichAsyncFunction[IN, OUT] extends AbstractRichFunction with AsyncFunction[IN, OUT]{
  override def setRuntimeContext(runtimeContext: RuntimeContext): Unit = {
    Preconditions.checkNotNull(runtimeContext)

    if (runtimeContext.isInstanceOf[IterationRuntimeContext]) {
      super.setRuntimeContext(new RichAsyncFunctionIterationRuntimeContext(
        runtimeContext.asInstanceOf[IterationRuntimeContext]));
    } else {
      super.setRuntimeContext(new RichAsyncFunctionRuntimeContext(runtimeContext));
    }
  }
}

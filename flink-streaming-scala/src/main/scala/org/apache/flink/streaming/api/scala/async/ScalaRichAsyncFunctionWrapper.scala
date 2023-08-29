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

import org.apache.flink.api.common.functions.{OpenContext, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture => JResultFuture, RichAsyncFunction => JRichAsyncFunction}

/**
 * A wrapper function that exposes a Scala RichAsyncFunction as a Java Rich Async Function.
 *
 * The Scala and Java RichAsyncFunctions differ in their type of "ResultFuture"
 *   - Scala RichAsyncFunction: [[org.apache.flink.streaming.api.scala.async.ResultFuture]]
 *   - Java RichAsyncFunction: [[org.apache.flink.streaming.api.functions.async.ResultFuture]]
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
final class ScalaRichAsyncFunctionWrapper[IN, OUT](func: RichAsyncFunction[IN, OUT])
  extends JRichAsyncFunction[IN, OUT] {

  override def asyncInvoke(input: IN, resultFuture: JResultFuture[OUT]): Unit = {
    func.asyncInvoke(input, new JavaResultFutureWrapper[OUT](resultFuture))
  }

  override def timeout(input: IN, resultFuture: JResultFuture[OUT]): Unit = {
    func.timeout(input, new JavaResultFutureWrapper[OUT](resultFuture))
  }

  override def open(openContext: OpenContext): Unit = {
    func.open(openContext)
  }

  override def close(): Unit = {
    func.close()
  }

  override def setRuntimeContext(runtimeContext: RuntimeContext): Unit = {
    super.setRuntimeContext(runtimeContext)
    func.setRuntimeContext(super.getRuntimeContext)
  }
}

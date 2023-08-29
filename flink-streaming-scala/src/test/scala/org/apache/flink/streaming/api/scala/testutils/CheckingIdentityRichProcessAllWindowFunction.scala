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
package org.apache.flink.streaming.api.scala.testutils

import org.apache.flink.api.common.functions.{OpenContext, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

class CheckingIdentityRichProcessAllWindowFunction[T, W <: Window]
  extends ProcessAllWindowFunction[T, T, W] {

  override def process(context: Context, input: Iterable[T], out: Collector[T]): Unit = {
    for (value <- input) {
      out.collect(value)
    }
  }

  override def open(openContext: OpenContext): Unit = {
    super.open(openContext)
    CheckingIdentityRichProcessAllWindowFunction.openCalled = true
  }

  override def close(): Unit = {
    super.close()
    CheckingIdentityRichProcessAllWindowFunction.closeCalled = true
  }

  override def setRuntimeContext(context: RuntimeContext): Unit = {
    super.setRuntimeContext(context)
    CheckingIdentityRichProcessAllWindowFunction.contextSet = true
  }
}

object CheckingIdentityRichProcessAllWindowFunction {

  @volatile
  private[CheckingIdentityRichProcessAllWindowFunction] var closeCalled = false

  @volatile
  private[CheckingIdentityRichProcessAllWindowFunction] var openCalled = false

  @volatile
  private[CheckingIdentityRichProcessAllWindowFunction] var contextSet = false

  def reset(): Unit = {
    closeCalled = false
    openCalled = false
    contextSet = false
  }

  def checkRichMethodCalls(): Unit = {
    if (!contextSet) {
      throw new AssertionError("context not set")
    }
    if (!openCalled) {
      throw new AssertionError("open() not called")
    }
    if (!closeCalled) {
      throw new AssertionError("close() not called")
    }
  }
}

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
package org.apache.flink.streaming.api.scala.function.util

import org.apache.flink.streaming.api.functions.windowing.{WindowFunction => JWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
 * A wrapper function that exposes a Scala Function4 as a Java WindowFunction.
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
final class ScalaWindowFunction[IN, OUT, KEY, W <: Window](
    private[this] val function: (KEY, W, Iterable[IN], Collector[OUT]) => Unit)
  extends JWindowFunction[IN, OUT, KEY, W] {

  @throws(classOf[Exception])
  override def apply(key: KEY, window: W, input: java.lang.Iterable[IN], out: Collector[OUT]) {
    function.apply(key, window, input.asScala, out)
  }
}

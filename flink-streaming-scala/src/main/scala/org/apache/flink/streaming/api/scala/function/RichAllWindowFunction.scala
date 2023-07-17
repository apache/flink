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

import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * Rich variant of the [[org.apache.flink.streaming.api.scala.function.AllWindowFunction]].
 *
 * As a [[org.apache.flink.api.common.functions.RichFunction]], it gives access to the
 * [[org.apache.flink.api.common.functions.RuntimeContext]] and provides setup and tear-down
 * methods.
 *
 * @tparam IN
 *   The type of the input value.
 * @tparam OUT
 *   The type of the output value.
 * @tparam W
 *   The type of Window that this window function can be applied on.
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
abstract class RichAllWindowFunction[IN, OUT, W <: Window]
  extends AbstractRichFunction
  with AllWindowFunction[IN, OUT, W] {}

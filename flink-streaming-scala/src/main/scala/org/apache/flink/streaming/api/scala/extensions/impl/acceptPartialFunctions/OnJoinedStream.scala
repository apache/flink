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
package org.apache.flink.streaming.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, JoinedStreams}
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * Wraps a joined data stream, allowing to use anonymous partial functions to perform extraction of
 * items in a tuple, case class instance or collection
 *
 * @param stream
 *   The wrapped data stream
 * @tparam L
 *   The type of the data stream items from the left side of the join
 * @tparam R
 *   The type of the data stream items from the right input of the join
 * @tparam K
 *   The type of key
 * @tparam W
 *   The type of the window
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
class OnJoinedStream[L, R, K, W <: Window](
    stream: JoinedStreams[L, R]#Where[K]#EqualTo#WithWindow[W]) {

  /**
   * Completes the join operation with the user function that is executed for windowed groups.
   *
   * @param fun
   *   The function that defines the projection of the join
   * @tparam O
   *   The return type of the projection, for which type information must be known
   * @return
   *   A fully joined data set of Os
   */
  @PublicEvolving
  def projecting[O: TypeInformation](fun: (L, R) => O): DataStream[O] =
    stream.apply(fun)

}

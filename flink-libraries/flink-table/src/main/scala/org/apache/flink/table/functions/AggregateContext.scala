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

package org.apache.flink.table.functions

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state._

/**
  * A AggregateContext allows to obtain global runtime information about the context in which the
  * aggregate function is executed. The information include the methods for accessing state.
  *
  * @param context the runtime context in which the Flink Function is executed
  */
class AggregateContext(context: RuntimeContext) {

  // ------------------------------------------------------------------------
  //  Methods for accessing state
  // ------------------------------------------------------------------------

  /**
    * Gets a handle to the [[ValueState]].
    *
    * @param stateProperties The descriptor defining the properties of the stats.
    * @tparam T The type of value stored in the state.
    * @return The partitioned state object.
    * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
    *                                       function (function is not part of a KeyedStream).
    */
  @PublicEvolving
  def getState[T](stateProperties: ValueStateDescriptor[T]): ValueState[T] =
  context.getState(stateProperties)

  /**
    * Gets a handle to the [[ListState]].
    *
    * @param stateProperties The descriptor defining the properties of the stats.
    * @tparam T The type of value stored in the state.
    * @return The partitioned state object.
    * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
    *                                       function (function is not part os a KeyedStream).
    */
  @PublicEvolving
  def getListState[T](stateProperties: ListStateDescriptor[T]): ListState[T] =
  context.getListState(stateProperties)

  /**
    * Gets a handle to the [[MapState]].
    *
    * @param stateProperties The descriptor defining the properties of the stats.
    * @tparam UK The type of the user keys stored in the state.
    * @tparam UV The type of the user values stored in the state.
    * @return The partitioned state object.
    * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
    *                                       function (function is not part of a KeyedStream).
    */
  @PublicEvolving
  def getMapState[UK, UV](stateProperties: MapStateDescriptor[UK, UV]): MapState[UK, UV] =
  context.getMapState(stateProperties)
}

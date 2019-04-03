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

package org.apache.flink.table.api.dataview

import java.lang.reflect.Field

import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, State, StateDescriptor}
import org.apache.flink.table.dataview.{ListViewTypeInfo, MapViewTypeInfo}

/**
  * Data view specification.
  *
  * @tparam ACC type extends [[DataView]]
  */
trait DataViewSpec[ACC <: DataView] {
  def stateId: String
  def field: Field
  def toStateDescriptor: StateDescriptor[_ <: State, _]
}

case class ListViewSpec[T](
    stateId: String,
    field: Field,
    listViewTypeInfo: ListViewTypeInfo[T])
  extends DataViewSpec[ListView[T]] {

  override def toStateDescriptor: StateDescriptor[_ <: State, _] =
    new ListStateDescriptor[T](stateId, listViewTypeInfo.elementType)
}

case class MapViewSpec[K, V](
    stateId: String,
    field: Field,
    mapViewTypeInfo: MapViewTypeInfo[K, V])
  extends DataViewSpec[MapView[K, V]] {

  override def toStateDescriptor: StateDescriptor[_ <: State, _] =
    new MapStateDescriptor[K, V](stateId, mapViewTypeInfo.keyType, mapViewTypeInfo.valueType)
}

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

import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, StateDescriptor}
import org.apache.flink.table.dataview.{ListViewTypeInfo, MapViewTypeInfo}

/**
  * Data view specification.
  *
  * @tparam ACC type extends [[DataView]]
  */
trait DataViewSpec[ACC <: DataView] {
  def id: String
  def field: Field
  def toStateDescriptor: StateDescriptor[_, _]
}

case class ListViewSpec[T](
    id: String,
    field: Field,
    listViewTypeInfo: ListViewTypeInfo[T])
  extends DataViewSpec[ListView[T]] {

  override def toStateDescriptor: StateDescriptor[_, _] =
    new ListStateDescriptor[T](id, listViewTypeInfo.elementType)
}

case class MapViewSpec[K, V](
    id: String,
    field: Field,
    mapViewTypeInfo: MapViewTypeInfo[K, V])
  extends DataViewSpec[MapView[K, V]] {

  override def toStateDescriptor: StateDescriptor[_, _] =
    new MapStateDescriptor[K, V](id, mapViewTypeInfo.keyType, mapViewTypeInfo.valueType)
}

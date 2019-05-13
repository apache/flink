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

package org.apache.flink.table.dataview

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.typeutils.{ListViewTypeInfo, MapViewTypeInfo}

/**
  * Data view specification.
  */
trait DataViewSpec {
  def stateId: String

  def fieldIndex: Int

  def dataViewTypeInfo: TypeInformation[_]

  def getStateDataViewClass(hasNamespace: Boolean): Class[_]

  def getCreateStateViewCall: String
}

case class ListViewSpec[T](
    stateId: String,
    fieldIndex: Int,
    dataViewTypeInfo: ListViewTypeInfo[T])
  extends DataViewSpec {

  // TODO implement it
  override def getStateDataViewClass(hasNamespace: Boolean): Class[_] = ???

  override def getCreateStateViewCall: String = "getStateListView"
}


case class MapViewSpec[K, V](
    stateId: String,
    fieldIndex: Int,
    dataViewTypeInfo: MapViewTypeInfo[K, V])
  extends DataViewSpec {

  // TODO implement it
  override def getStateDataViewClass(hasNamespace: Boolean): Class[_] = ???

  override def getCreateStateViewCall: String = "getStateMapView"
}
